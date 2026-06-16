package batman

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
    private val state = ConcurrentHashMap<GroupId, ConcurrentHashMap<String, LinkEntry>>()

    /**
     * Records [link] as part of the spanning tree for [groupId], or refreshes
     * its [LinkEntry.lastSeen] timestamp if it was already registered.
     *
     * Called by:
     *   - Relay nodes when forwarding a [Frame.BeaconFrame] (for both the
     *     incoming link and the outgoing link toward the owner).
     *   - The group owner when a beacon arrives at its final destination.
     *   - Group members at each beacon-loop tick to register the outgoing link
     *     toward the owner.
     */
    fun registerLink(groupId: GroupId, link: Link) {
        state.getOrPut(groupId) { ConcurrentHashMap() }
            .compute(link.id) { _, existing ->
                // Refresh timestamp if present; create a new entry otherwise.
                existing?.also { it.lastSeen = Instant.now() } ?: LinkEntry(link, Instant.now())
            }
    }

    /**
     * Returns all links registered in [groupId]'s tree, excluding [except].
     *
     * Used when relaying a [Frame.MulticastFrame]: the [except] link is the one
     * the frame arrived on, so excluding it prevents the frame from being sent
     * back in the direction it came from.
     */
    fun linksFor(groupId: GroupId, except: Link): Set<Link> =
        state[groupId]?.values
            ?.filter { it.link.id != except.id }
            ?.map { it.link }
            ?.toSet() ?: emptySet()

    /**
     * Returns all links registered in [groupId]'s tree with no exclusions.
     *
     * Used when a node originates a [Frame.MulticastFrame] (i.e. it is not
     * relaying — there is no "incoming link" to exclude).
     */
    fun allLinksFor(groupId: GroupId): Set<Link> =
        state[groupId]?.values?.map { it.link }?.toSet() ?: emptySet()

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
        state.forEach { (groupId, links) ->
            links.entries.removeIf { it.value.lastSeen.isBefore(cutoff) }
            if (links.isEmpty()) state.remove(groupId)
        }
    }
}
