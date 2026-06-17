package batman

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class MulticastTreeTest {

    // Minimal Link stub — only id matters for tree operations.
    private fun link(id: String): Link = object : Link {
        override val id = id
        override val ogmInterval: Duration = 1.seconds
        override suspend fun broadcast(frame: ByteArray) = Unit
        override val frames: Flow<ByteArray> = emptyFlow()
    }

    private val gid = GroupId(owner = 1u, seq = 0u)
    private val linkA = link("A")
    private val linkB = link("B")
    private val linkC = link("C")

    // ── registerLink / linksFor ───────────────────────────────────────────────

    @Test fun `registered link appears in linksFor`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        assertTrue(linkA in tree.linksFor(gid, except = linkB))
    }

    @Test fun `linksFor excludes the except link`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        tree.registerLink(gid, linkB)
        val result = tree.linksFor(gid, except = linkA)
        assertFalse(linkA in result)
        assertTrue(linkB in result)
    }

    @Test fun `linksFor returns empty set for unknown group`() {
        val tree = MulticastTree()
        assertEquals(emptySet(), tree.linksFor(GroupId(99u, 0u), except = linkA))
    }

    @Test fun `linksFor returns empty set when only the except link is registered`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        assertEquals(emptySet(), tree.linksFor(gid, except = linkA))
    }

    @Test fun `multiple links all returned except the excluded one`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        tree.registerLink(gid, linkB)
        tree.registerLink(gid, linkC)
        val result = tree.linksFor(gid, except = linkA)
        assertEquals(setOf(linkB, linkC), result)
    }

    // ── allLinksFor ───────────────────────────────────────────────────────────

    @Test fun `allLinksFor returns all registered links`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        tree.registerLink(gid, linkB)
        assertEquals(setOf(linkA, linkB), tree.allLinksFor(gid))
    }

    @Test fun `allLinksFor returns empty set for unknown group`() {
        assertEquals(emptySet(), MulticastTree().allLinksFor(GroupId(99u, 0u)))
    }

    // ── registerLink refreshes lastSeen ───────────────────────────────────────

    @Test fun `re-registering a link keeps it in the tree`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        tree.registerLink(gid, linkA)  // refresh
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
    }

    // ── independent groups ────────────────────────────────────────────────────

    @Test fun `different groups have independent link sets`() {
        val tree = MulticastTree()
        val gid2 = GroupId(owner = 2u, seq = 0u)
        tree.registerLink(gid,  linkA)
        tree.registerLink(gid2, linkB)
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
        assertEquals(setOf(linkB), tree.allLinksFor(gid2))
    }

    // ── evictStale ────────────────────────────────────────────────────────────

    @Test fun `evictStale removes links older than threshold`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        // Evict with a zero-duration threshold — everything is immediately stale.
        Thread.sleep(5)
        tree.evictStale(0.seconds)
        assertEquals(emptySet(), tree.allLinksFor(gid))
    }

    @Test fun `evictStale removes empty group entries`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        Thread.sleep(5)
        tree.evictStale(0.seconds)
        // Group should be gone; allLinksFor returns empty without NPE.
        assertEquals(emptySet(), tree.allLinksFor(gid))
    }

    @Test fun `evictStale keeps recently refreshed links`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        // Evict with a large threshold — nothing should be removed.
        tree.evictStale(60.seconds)
        assertEquals(setOf(linkA), tree.allLinksFor(gid))
    }

    @Test fun `evictStale removes stale but keeps fresh link in same group`() {
        val tree = MulticastTree()
        tree.registerLink(gid, linkA)
        Thread.sleep(20)
        tree.registerLink(gid, linkB)  // registered after the sleep — fresh
        tree.evictStale(10.seconds / 1000)  // threshold = 10 ms
        val remaining = tree.allLinksFor(gid)
        assertFalse(linkA in remaining, "stale linkA should be evicted")
        assertTrue(linkB in remaining,  "fresh linkB should be kept")
    }
}
