package kiro

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Maintains the per-group multicast spanning tree used to forward [Frame.MulticastFrame]s
 * without flooding the entire network.
 *
 * ## How the tree is built
 *
 * Each group member periodically sends a [Frame.BeaconFrame] toward the group owner
 * via unicast routing. Every node that relays this beacon is on the path between
 * that member and the owner. The relay node calls [registerLink] for both the
 * incoming link (where the beacon arrived) and the outgoing link (where it is
 * forwarded). This progressively builds a spanning tree rooted at the owner:
 *
 *   Owner
 *   ├── Link A  (registered when M1's beacon arrived from M1 direction)
 *   └── Link B  (registered when M2's beacon arrived from M2 direction)
 *       └── Link C  (registered by an intermediate node between M2 and the owner)
 *
 * ## How the tree is used
 *
 * When a [Frame.MulticastFrame] arrives on a link, [linksFor] returns all other
 * links registered for that group, excluding the one the frame came from (to
 * prevent echo). The frame is re-enqueued on each of those links, efficiently
 * distributing it to all branches of the tree.
 *
 * ## Stale entry eviction
 *
 * Each [LinkEntry] records [LinkEntry.lastSeen] — the last time a beacon refreshed
 * this link's membership in the tree. [evictStale] removes entries older than a
 * configurable threshold (typically 3× the beacon interval). If a member stops
 * sending beacons (e.g. it left the group or lost connectivity), its branch is
 * automatically pruned, preventing stale state from consuming bandwidth.
 *
 * ## Thread safety
 *
 * [ConcurrentHashMap.compute] is used for atomic upsert of [LinkEntry] objects.
 * The outer map is also a [ConcurrentHashMap], so concurrent registration and
 * eviction across multiple coroutines is safe without additional locking.
 */
class MulticastTree {

    /**
     * A single link registered as part of a group's spanning tree.
     *
     * @property link The [Link] interface recorded in the tree.
     * @property lastSeen When this entry was last refreshed by a beacon relay.
     *   Marked [@Volatile] so that [evictStale] reads are visible across threads
     *   without requiring full synchronisation.
     */
    private data class LinkEntry(val link: Link, @Volatile var lastSeen: Instant)

    /**
     * Outer key: [GroupId].
     * Inner key: [Link.id] (String) — using the string ID rather than the Link
     * object avoids identity-equality pitfalls if Link instances are recreated.
     */
    private val upstream   = ConcurrentHashMap<GroupId, ConcurrentHashMap<String, LinkEntry>>()
    private val downstream = ConcurrentHashMap<GroupId, ConcurrentHashMap<String, LinkEntry>>()

    private fun upsert(
        map: ConcurrentHashMap<GroupId, ConcurrentHashMap<String, LinkEntry>>,
        groupId: GroupId,
        link: Link
    ) {
        map.getOrPut(groupId) { ConcurrentHashMap() }
            .compute(link.id) { _, existing ->
                existing?.also { it.lastSeen = Instant.now() } ?: LinkEntry(link, Instant.now())
            }
    }

    /**
     * Records [link] as the sole upstream link for [groupId], replacing any previously
     * registered upstream link. A node has exactly one upstream path (its current best
     * route to the active root), so replacing rather than accumulating prevents stale
     * upstream links from causing duplicate multicast transmissions after a route change.
     */
    fun registerUpstream(groupId: GroupId, link: Link) {
        upstream.compute(groupId) { _, existing ->
            val current = existing?.get(link.id)
            if (current != null) {
                // Same link already registered — refresh timestamp in place.
                current.lastSeen = Instant.now()
                existing
            } else {
                // Route changed or first registration — replace the inner map entirely
                // so no stale upstream links from a previous route remain.
                ConcurrentHashMap<String, LinkEntry>().also { it[link.id] = LinkEntry(link, Instant.now()) }
            }
        }
    }

    /**
     * Records [link] as a downstream link for [groupId] — the direction toward
     * group members. Called by a relay node when a beacon arrives from below.
     *
     * The presence of at least one downstream link for a group means another node
     * further down the tree is already keeping this node's path to the root alive,
     * so the relay node itself does not need to originate its own beacon.
     */
    fun registerDownstream(groupId: GroupId, link: Link) = upsert(downstream, groupId, link)

    /**
     * Returns `true` if there is at least one downstream link for [groupId] whose
     * [LinkEntry.lastSeen] is no older than [within].
     *
     * Used by [KiroRouter.beaconLoop] to decide whether this node can suppress
     * its own beacon: if a downstream member is actively sending beacons through
     * this node, its relayed beacons are already keeping the upstream path alive.
     */
    fun hasActiveDownstream(groupId: GroupId, within: Duration): Boolean {
        val cutoff = Instant.now().minus(within.toJavaDuration())
        return downstream[groupId]?.values?.any { it.lastSeen.isAfter(cutoff) } == true
    }

    /**
     * Returns all links registered in [groupId]'s tree (both directions), excluding [except].
     *
     * Used when relaying a [Frame.MulticastFrame]: the [except] link is the one
     * the frame arrived on, so excluding it prevents the frame from being sent
     * back in the direction it came from.
     */
    fun linksFor(groupId: GroupId, except: Link): Set<Link> {
        val all = mutableMapOf<String, Link>()
        upstream[groupId]?.values?.forEach   { all[it.link.id] = it.link }
        downstream[groupId]?.values?.forEach { all[it.link.id] = it.link }
        return all.values.filter { it.id != except.id }.toSet()
    }

    /**
     * Returns all links registered in [groupId]'s tree with no exclusions.
     *
     * Used when a node originates a [Frame.MulticastFrame] (i.e. it is not
     * relaying — there is no "incoming link" to exclude).
     */
    fun allLinksFor(groupId: GroupId): Set<Link> {
        val all = mutableMapOf<String, Link>()
        upstream[groupId]?.values?.forEach   { all[it.link.id] = it.link }
        downstream[groupId]?.values?.forEach { all[it.link.id] = it.link }
        return all.values.toSet()
    }

    /**
     * Removes all [LinkEntry] records whose [LinkEntry.lastSeen] is older than
     * [threshold], and removes any group entries that become empty as a result.
     *
     * Should be called periodically (e.g. every [threshold]/3) by a background
     * coroutine. A member that stops sending beacons will have its branch pruned
     * after at most [threshold] time, keeping the tree consistent with the live
     * membership.
     */
    fun evictStale(threshold: Duration) {
        val cutoff = Instant.now().minus(threshold.toJavaDuration())
        for (map in listOf(upstream, downstream)) {
            map.forEach { (groupId, links) ->
                links.entries.removeIf { it.value.lastSeen.isBefore(cutoff) }
                if (links.isEmpty()) map.remove(groupId)
            }
        }
    }
}
