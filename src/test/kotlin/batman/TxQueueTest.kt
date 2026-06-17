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
        priority: TxPriority,
        payload: Byte = 0
    ) = TxEntry(frame = byteArrayOf(payload), targetLink = link, priority = priority)

    // ── priority ordering ─────────────────────────────────────────────────────

    @Test fun `CONTROL frame dequeued before DATA frame when enqueued in reverse order`() = runTest {
        val q = TxQueue()
        val data    = entry(linkA, TxPriority.DATA,    payload = 1)
        val control = entry(linkA, TxPriority.CONTROL, payload = 2)
        q.enqueue(data)
        q.enqueue(control)
        assertEquals(control, q.pollFor(linkA))
        assertEquals(data,    q.pollFor(linkA))
    }

    @Test fun `two CONTROL frames dequeued FIFO`() = runTest {
        val q = TxQueue()
        val first  = entry(linkA, TxPriority.CONTROL, payload = 1)
        val second = entry(linkA, TxPriority.CONTROL, payload = 2)
        q.enqueue(first)
        q.enqueue(second)
        assertEquals(first,  q.pollFor(linkA))
        assertEquals(second, q.pollFor(linkA))
    }

    @Test fun `two DATA frames dequeued FIFO`() = runTest {
        val q = TxQueue()
        val first  = entry(linkA, TxPriority.DATA, payload = 1)
        val second = entry(linkA, TxPriority.DATA, payload = 2)
        q.enqueue(first)
        q.enqueue(second)
        assertEquals(first,  q.pollFor(linkA))
        assertEquals(second, q.pollFor(linkA))
    }

    // ── per-link isolation ────────────────────────────────────────────────────

    @Test fun `pollFor only returns frames destined for the requested link`() = runTest {
        val q = TxQueue()
        val forB = entry(linkB, TxPriority.DATA, payload = 0xBB.toByte())
        val forA = entry(linkA, TxPriority.DATA, payload = 0xAA.toByte())
        q.enqueue(forB)
        q.enqueue(forA)
        assertEquals(forA, q.pollFor(linkA))
        assertEquals(forB, q.pollFor(linkB))
    }

    // ── suspension and wakeup ─────────────────────────────────────────────────

    @Test fun `pollFor suspends and wakes when frame is enqueued`() = runTest {
        val q = TxQueue()
        val expected = entry(linkA, TxPriority.DATA)

        val deferred = async { q.pollFor(linkA) }
        // Yield to let the async block suspend on pollFor, then enqueue.
        launch { q.enqueue(expected) }

        assertEquals(expected, deferred.await())
    }

    @Test fun `direct handoff when waiter is already registered`() = runTest {
        val q = TxQueue()
        val expected = entry(linkA, TxPriority.CONTROL)

        // pollFor suspends first (no frame available), then enqueue delivers directly.
        val deferred = async { q.pollFor(linkA) }
        q.enqueue(expected)

        assertEquals(expected, deferred.await())
    }

    // ── mixed links and priorities ────────────────────────────────────────────

    @Test fun `CONTROL on linkA does not affect linkB queue`() = runTest {
        val q = TxQueue()
        val ctrlA = entry(linkA, TxPriority.CONTROL)
        val dataB = entry(linkB, TxPriority.DATA)
        q.enqueue(ctrlA)
        q.enqueue(dataB)
        assertEquals(dataB, q.pollFor(linkB))  // linkB gets its own frame regardless
        assertEquals(ctrlA, q.pollFor(linkA))
    }

    @Test fun `priority ordering is respected across many enqueues`() = runTest {
        val q = TxQueue()
        repeat(3) { q.enqueue(entry(linkA, TxPriority.DATA,    payload = it.toByte())) }
        repeat(3) { q.enqueue(entry(linkA, TxPriority.CONTROL, payload = (it + 10).toByte())) }

        val results = List(6) { q.pollFor(linkA) }
        val priorities = results.map { it.priority }

        // All CONTROL frames must come before any DATA frame.
        val firstData = priorities.indexOfFirst { it == TxPriority.DATA }
        val lastControl = priorities.indexOfLast { it == TxPriority.CONTROL }
        assert(lastControl < firstData) {
            "Expected all CONTROL before DATA, got: $priorities"
        }
    }
}
