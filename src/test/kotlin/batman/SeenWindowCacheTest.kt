package batman

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SeenWindowCacheTest {

    private val node: NodeId = 1u

    private fun cache(windowSize: Int = 64) = SeenWindowCache(windowSize)

    // ── basic acceptance ──────────────────────────────────────────────────────

    @Test fun `first packet from new originator is always accepted`() {
        assertTrue(cache().markIfNew(node, 42u))
    }

    @Test fun `same packet twice is rejected on second call`() {
        val c = cache()
        assertTrue(c.markIfNew(node, 10u))
        assertFalse(c.markIfNew(node, 10u))
    }

    @Test fun `sequential packets are all accepted`() {
        val c = cache()
        for (seq in 0u..9u) {
            assertTrue(c.markIfNew(node, seq.toUShort()), "seq $seq should be new")
        }
    }

    // ── out-of-order within window ────────────────────────────────────────────

    @Test fun `out-of-order packet within window is accepted`() {
        val c = cache()
        c.markIfNew(node, 10u)  // advance window to 10
        assertTrue(c.markIfNew(node, 8u))   // 2 behind — within window
    }

    @Test fun `out-of-order duplicate within window is rejected`() {
        val c = cache()
        c.markIfNew(node, 10u)
        c.markIfNew(node, 8u)
        assertFalse(c.markIfNew(node, 8u))
    }

    @Test fun `packet just outside the window is rejected`() {
        val c = cache(windowSize = 8)
        c.markIfNew(node, 10u)  // latest = 10, window covers 10..3
        assertFalse(c.markIfNew(node, 2u))  // 8 behind — outside window of size 8
    }

    @Test fun `packet at window boundary is accepted`() {
        val c = cache(windowSize = 8)
        c.markIfNew(node, 10u)
        assertTrue(c.markIfNew(node, 3u))   // exactly 7 behind (offset = windowSize - 1)
    }

    // ── large gap clears window ───────────────────────────────────────────────

    @Test fun `gap larger than window clears history and accepts new seq`() {
        val c = cache(windowSize = 8)
        c.markIfNew(node, 5u)
        // Jump forward by more than window size
        assertTrue(c.markIfNew(node, 20u))
    }

    @Test fun `after large gap old seq is still rejected because it falls outside the new window`() {
        val c = cache(windowSize = 8)
        c.markIfNew(node, 5u)
        c.markIfNew(node, 20u)  // gap=15 >= windowSize(8): window cleared, latest=20
        // seq 5 is 15 positions behind latest (20), still >= windowSize → outside window
        assertFalse(c.markIfNew(node, 5u))
    }

    // ── UShort wraparound ─────────────────────────────────────────────────────

    @Test fun `wraparound from 65535 to 0 is accepted as newer`() {
        val c = cache()
        c.markIfNew(node, 65535u)
        assertTrue(c.markIfNew(node, 0u))
    }

    @Test fun `wraparound sequence 65534 65535 0 1 all accepted`() {
        val c = cache()
        for (seq in listOf<UShort>(65534u, 65535u, 0u, 1u)) {
            assertTrue(c.markIfNew(node, seq), "seq $seq should be new")
        }
    }

    @Test fun `duplicate after wraparound is rejected`() {
        val c = cache()
        c.markIfNew(node, 65535u)
        c.markIfNew(node, 0u)
        assertFalse(c.markIfNew(node, 0u))
    }

    @Test fun `seq before wraparound is treated as older when window has advanced past 0`() {
        val c = cache(windowSize = 8)
        c.markIfNew(node, 65535u)
        c.markIfNew(node, 0u)
        c.markIfNew(node, 1u)
        // 65534 is 3 behind the current latest (1), within the window
        assertTrue(c.markIfNew(node, 65534u))
        // Asking again should reject it
        assertFalse(c.markIfNew(node, 65534u))
    }

    // ── independent originators ───────────────────────────────────────────────

    @Test fun `different originators have independent windows`() {
        val c = cache()
        val nodeA: NodeId = 1u
        val nodeB: NodeId = 2u
        c.markIfNew(nodeA, 10u)
        // Same seq from a different originator should be accepted
        assertTrue(c.markIfNew(nodeB, 10u))
    }

    @Test fun `duplicate for one originator does not affect another`() {
        val c = cache()
        val nodeA: NodeId = 1u
        val nodeB: NodeId = 2u
        c.markIfNew(nodeA, 5u)
        c.markIfNew(nodeB, 5u)
        assertFalse(c.markIfNew(nodeA, 5u))
        assertFalse(c.markIfNew(nodeB, 5u))
    }
}
