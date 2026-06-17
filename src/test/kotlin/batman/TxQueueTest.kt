package batman

import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class TxQueueTest {

    private fun link(id: String): Link = object : Link {
        override val id = id
        override val ogmInterval: Duration = 1.seconds
        override suspend fun broadcast(frame: ByteArray) = Unit
        override val frames: Flow<ByteArray> = emptyFlow()
    }

    private val linkA = link("A")
    private val linkB = link("B")

    private fun entry(
        link: Link,
        flavor: PacketFlavor,
        payload: Byte = 0
    ) = TxEntry(frame = byteArrayOf(payload), eligibleLinks = setOf(link), flavor = flavor)

    // ── priority ordering ─────────────────────────────────────────────────────

    @Test fun `CONTROL frame dequeued before DATA frame when enqueued in reverse order`() = runTest {
        val q = TxQueue()
        val data    = entry(linkA, PacketFlavor.DATA,    payload = 1)
        val control = entry(linkA, PacketFlavor.OGM, payload = 2)
        q.enqueue(data)
        q.enqueue(control)
        assertEquals(control, q.pollFor(linkA))
        assertEquals(data,    q.pollFor(linkA))
    }

    @Test fun `two CONTROL frames dequeued FIFO`() = runTest {
        val q = TxQueue()
        val first  = entry(linkA, PacketFlavor.OGM, payload = 1)
        val second = entry(linkA, PacketFlavor.OGM, payload = 2)
        q.enqueue(first)
        q.enqueue(second)
        assertEquals(first,  q.pollFor(linkA))
        assertEquals(second, q.pollFor(linkA))
    }

    @Test fun `two DATA frames dequeued FIFO`() = runTest {
        val q = TxQueue()
        val first  = entry(linkA, PacketFlavor.DATA, payload = 1)
        val second = entry(linkA, PacketFlavor.DATA, payload = 2)
        q.enqueue(first)
        q.enqueue(second)
        assertEquals(first,  q.pollFor(linkA))
        assertEquals(second, q.pollFor(linkA))
    }

    // ── per-link isolation ────────────────────────────────────────────────────

    @Test fun `pollFor only returns frames destined for the requested link`() = runTest {
        val q = TxQueue()
        val forB = entry(linkB, PacketFlavor.DATA, payload = 0xBB.toByte())
        val forA = entry(linkA, PacketFlavor.DATA, payload = 0xAA.toByte())
        q.enqueue(forB)
        q.enqueue(forA)
        assertEquals(forA, q.pollFor(linkA))
        assertEquals(forB, q.pollFor(linkB))
    }

    // ── suspension and wakeup ─────────────────────────────────────────────────

    @Test fun `pollFor suspends and wakes when frame is enqueued`() = runTest {
        val q = TxQueue()
        val expected = entry(linkA, PacketFlavor.DATA)

        val deferred = async { q.pollFor(linkA) }
        // Yield to let the async block suspend on pollFor, then enqueue.
        launch { q.enqueue(expected) }

        assertEquals(expected, deferred.await())
    }

    @Test fun `direct handoff when waiter is already registered`() = runTest {
        val q = TxQueue()
        val expected = entry(linkA, PacketFlavor.OGM)

        // pollFor suspends first (no frame available), then enqueue delivers directly.
        val deferred = async { q.pollFor(linkA) }
        q.enqueue(expected)

        assertEquals(expected, deferred.await())
    }

    // ── mixed links and priorities ────────────────────────────────────────────

    @Test fun `CONTROL on linkA does not affect linkB queue`() = runTest {
        val q = TxQueue()
        val ctrlA = entry(linkA, PacketFlavor.OGM)
        val dataB = entry(linkB, PacketFlavor.DATA)
        q.enqueue(ctrlA)
        q.enqueue(dataB)
        assertEquals(dataB, q.pollFor(linkB))  // linkB gets its own frame regardless
        assertEquals(ctrlA, q.pollFor(linkA))
    }

    // ── multi-link eligible set ───────────────────────────────────────────────

    @Test fun `frame eligible for two links is claimed by whichever polls first`() = runTest {
        val q = TxQueue()
        val multiFrame = TxEntry(byteArrayOf(0x42), eligibleLinks = setOf(linkA, linkB), flavor = PacketFlavor.DATA)
        q.enqueue(multiFrame)

        // linkB polls first — it should claim the frame
        val claimed = q.pollFor(linkB)
        assertEquals(multiFrame, claimed)
    }

    @Test fun `frame is delivered exactly once across competing links`() = runTest {
        val q = TxQueue()
        val multiFrame = TxEntry(byteArrayOf(0x42), eligibleLinks = setOf(linkA, linkB), flavor = PacketFlavor.DATA)
        val singleB    = TxEntry(byteArrayOf(0x99.toByte()), eligibleLinks = setOf(linkB),         flavor = PacketFlavor.DATA)
        q.enqueue(multiFrame)
        q.enqueue(singleB)

        // linkA claims the multi-link frame
        assertEquals(multiFrame, q.pollFor(linkA))
        // linkB must now get singleB, not the already-claimed multiFrame
        assertEquals(singleB, q.pollFor(linkB))
    }

    @Test fun `enqueue wakes a waiter on any eligible link`() = runTest {
        val q = TxQueue()
        // linkA suspends waiting for a frame
        val deferred = async { q.pollFor(linkA) }
        // Frame eligible for both links — should wake linkA's waiter directly
        val multiFrame = TxEntry(byteArrayOf(0x77), eligibleLinks = setOf(linkA, linkB), flavor = PacketFlavor.OGM)
        q.enqueue(multiFrame)
        assertEquals(multiFrame, deferred.await())
    }

    @Test fun `priority still respected within multi-link eligible set`() = runTest {
        val q = TxQueue()
        val data    = TxEntry(byteArrayOf(1), eligibleLinks = setOf(linkA, linkB), flavor = PacketFlavor.DATA)
        val control = TxEntry(byteArrayOf(2), eligibleLinks = setOf(linkA, linkB), flavor = PacketFlavor.OGM)
        q.enqueue(data)
        q.enqueue(control)

        assertEquals(control, q.pollFor(linkA))
        assertEquals(data,    q.pollFor(linkB))
    }

    @Test fun `priority ordering is respected across many enqueues`() = runTest {
        val q = TxQueue()
        repeat(3) { q.enqueue(entry(linkA, PacketFlavor.DATA,    payload = it.toByte())) }
        repeat(3) { q.enqueue(entry(linkA, PacketFlavor.OGM, payload = (it + 10).toByte())) }

        val results = List(6) { q.pollFor(linkA) }
        val flavors = results.map { it.flavor }

        // All control flavors must come before any data flavor.
        val firstData    = flavors.indexOfFirst { !it.isControl }
        val lastControl  = flavors.indexOfLast  {  it.isControl }
        assert(lastControl < firstData) {
            "Expected all control flavors before data flavors, got: $flavors"
        }
    }
}
