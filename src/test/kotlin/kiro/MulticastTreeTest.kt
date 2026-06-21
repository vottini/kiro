package kiro

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MulticastTreeTest {

    // Minimal Link stub — only id matters for tree operations.
    private fun link(id: String): Link = object : Link {
        override val id = id
        override val ogmInterval: Duration = 1.seconds
        override suspend fun broadcast(frame: ByteArray) = Unit
        override val frames: Flow<ByteArray> = emptyFlow()
    }

    private val gid = GroupId(1u)
    private val linkA = link("A")
    private val linkB = link("B")
    private val linkC = link("C")

    // ── registerUpstream / linksFor ───────────────────────────────────────────

    @Test fun `registered upstream link appears in linksFor`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        assertTrue(linkA in tree.linksFor(gid, except = linkB))
    }

    @Test fun `linksFor excludes the except link`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerUpstream(gid, linkB)
        val result = tree.linksFor(gid, except = linkA)
        assertFalse(linkA in result)
        assertTrue(linkB in result)
    }

    @Test fun `linksFor returns empty set for unknown group`() {
        val tree = MulticastTree()
        assertEquals(emptySet(), tree.linksFor(GroupId(99u), except = linkA))
    }

    @Test fun `linksFor returns empty set when only the except link is registered`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        assertEquals(emptySet(), tree.linksFor(gid, except = linkA))
    }

    @Test fun `multiple downstream links all returned except the excluded one`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerDownstream(gid, linkB)
        tree.registerDownstream(gid, linkC)
        val result = tree.linksFor(gid, except = linkA)
        assertEquals(setOf(linkB, linkC), result)
    }

    @Test fun `linksFor merges upstream and downstream links`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerDownstream(gid, linkB)
        assertEquals(setOf(linkA, linkB), tree.linksFor(gid, except = linkC))
    }

    // ── allLinksFor ───────────────────────────────────────────────────────────

    @Test fun `allLinksFor returns all registered links`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerDownstream(gid, linkB)
        assertEquals(setOf(linkA, linkB), tree.allLinksFor(gid))
    }

    @Test fun `allLinksFor merges upstream and downstream`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerDownstream(gid, linkB)
        assertEquals(setOf(linkA, linkB), tree.allLinksFor(gid))
    }

    @Test fun `registerUpstream replaces previous upstream link on route change`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerUpstream(gid, linkB)   // route changed: linkA should be evicted immediately
        assertEquals(setOf(linkB), tree.allLinksFor(gid))
    }

    @Test fun `allLinksFor deduplicates a link registered in both directions`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerDownstream(gid, linkA)
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
    }

    @Test fun `allLinksFor returns empty set for unknown group`() {
        assertEquals(emptySet(), MulticastTree().allLinksFor(GroupId(99u)))
    }

    // ── re-registration refreshes lastSeen ────────────────────────────────────

    @Test fun `re-registering a link keeps it in the tree`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.registerUpstream(gid, linkA)  // refresh
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
    }

    // ── independent groups ────────────────────────────────────────────────────

    @Test fun `different groups have independent link sets`() {
        val tree = MulticastTree()
        val gid2 = GroupId(2u)
        tree.registerUpstream(gid,  linkA)
        tree.registerUpstream(gid2, linkB)
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
        assertEquals(setOf(linkB), tree.allLinksFor(gid2))
    }

    // ── hasActiveDownstream ───────────────────────────────────────────────────

    @Test fun `hasActiveDownstream returns false when no downstream registered`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        assertFalse(tree.hasActiveDownstream(gid, 60.seconds))
    }

    @Test fun `hasActiveDownstream returns true immediately after registration`() {
        val tree = MulticastTree()
        tree.registerDownstream(gid, linkA)
        assertTrue(tree.hasActiveDownstream(gid, 60.seconds))
    }

    @Test fun `hasActiveDownstream returns false after downstream entry becomes stale`() {
        val tree = MulticastTree()
        tree.registerDownstream(gid, linkA)
        Thread.sleep(20)
        assertFalse(tree.hasActiveDownstream(gid, 10.milliseconds))
    }

    @Test fun `hasActiveDownstream returns true if any downstream link is fresh`() {
        val tree = MulticastTree()
        tree.registerDownstream(gid, linkA)
        Thread.sleep(20)
        tree.registerDownstream(gid, linkB)  // linkB is fresh
        assertTrue(tree.hasActiveDownstream(gid, 10.milliseconds))
    }

    // ── evictStale ────────────────────────────────────────────────────────────

    @Test fun `evictStale removes links older than threshold`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        Thread.sleep(5)
        tree.evictStale(0.seconds)
        assertEquals(emptySet(), tree.allLinksFor(gid))
    }

    @Test fun `evictStale removes empty group entries`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        Thread.sleep(5)
        tree.evictStale(0.seconds)
        assertEquals(emptySet(), tree.allLinksFor(gid))
    }

    @Test fun `evictStale keeps recently refreshed links`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        tree.evictStale(60.seconds)
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
    }

    @Test fun `evictStale removes stale but keeps fresh link in same group`() {
        val tree = MulticastTree()
        tree.registerUpstream(gid, linkA)
        Thread.sleep(20)
        tree.registerUpstream(gid, linkB)  // registered after the sleep — fresh
        tree.evictStale(10.seconds / 1000)  // threshold = 10 ms
        val remaining = tree.allLinksFor(gid)
        assertFalse(linkA in remaining, "stale linkA should be evicted")
        assertTrue(linkB in remaining,  "fresh linkB should be kept")
    }

    @Test fun `evictStale evicts stale downstream links too`() {
        val tree = MulticastTree()
        tree.registerDownstream(gid, linkA)
        Thread.sleep(20)
        tree.evictStale(10.milliseconds)
        assertFalse(tree.hasActiveDownstream(gid, 60.seconds))
    }
}
