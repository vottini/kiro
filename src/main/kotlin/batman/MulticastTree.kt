package batman

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class MulticastTree {
    private data class LinkEntry(val link: Link, @Volatile var lastSeen: Instant)

    private val state = ConcurrentHashMap<GroupId, ConcurrentHashMap<String, LinkEntry>>()

    fun registerLink(groupId: GroupId, link: Link) {
        state.getOrPut(groupId) { ConcurrentHashMap() }
            .compute(link.id) { _, existing ->
                existing?.also { it.lastSeen = Instant.now() } ?: LinkEntry(link, Instant.now())
            }
    }

    // All links in the group's tree except the one the packet arrived on
    fun linksFor(groupId: GroupId, except: Link): Set<Link> =
        state[groupId]?.values
            ?.filter { it.link.id != except.id }
            ?.map { it.link }
            ?.toSet() ?: emptySet()

    // All links in the group's tree (for originating a multicast)
    fun allLinksFor(groupId: GroupId): Set<Link> =
        state[groupId]?.values?.map { it.link }?.toSet() ?: emptySet()

    fun evictStale(threshold: Duration) {
        val cutoff = Instant.now().minus(threshold.toJavaDuration())
        state.forEach { (groupId, links) ->
            links.entries.removeIf { it.value.lastSeen.isBefore(cutoff) }
            if (links.isEmpty()) state.remove(groupId)
        }
    }
}
