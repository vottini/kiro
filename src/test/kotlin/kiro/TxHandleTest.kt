package kiro

import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class TxHandleTest {

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
        payload: Byte = 0,
        userPriority: Int = 0
    ) = TxEntry(
        frame = byteArrayOf(payload),
        eligibleLinks = setOf(link),
        flavor = flavor,
        userPriority = userPriority
    )

    // ── handle identity ───────────────────────────────────────────────────────

    @Test fun `handle id matches entry insertionOrder`() = runTest {
        val q = TxQueue()
        val e = entry(linkA, PacketFlavor.DATA)
        val h = q.enqueue(e)
        assertEquals(e.insertionOrder, h.id)
    }

    // ── isSent ────────────────────────────────────────────────────────────────

    @Test fun `isSent is false while entry is still queued`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        assertFalse(h.isSent)
    }

    @Test fun `isSent is true after pollFor claims the entry`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        q.pollFor(linkA)
        assertTrue(h.isSent)
    }

    @Test fun `isSent is true immediately when entry is delivered to a waiting pollFor`() = runTest {
        val q = TxQueue()
        val deferred = async { q.pollFor(linkA) }
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        deferred.await()
        assertTrue(h.isSent)
    }

    // ── onSent — non-suspending callback ─────────────────────────────────────

    @Test fun `onSent non-suspending callback does not fire before pollFor`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        var called = false
        h.onSent { called = true }
        assertFalse(called)
    }

    @Test fun `onSent non-suspending callback fires synchronously when entry is transmitted`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        var called = false
        h.onSent { called = true }
        q.pollFor(linkA)
        assertTrue(called)
    }

    @Test fun `onSent non-suspending fires immediately when entry was already sent`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        q.pollFor(linkA)
        var called = false
        h.onSent { called = true }   // entry already sent — should fire right away
        assertTrue(called)
    }

    // ── awaitSent ─────────────────────────────────────────────────────────────

    @Test fun `awaitSent suspends until pollFor claims the entry`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        var reached = false
        launch {
            h.awaitSent()
            reached = true
        }
        yield()                 // let the launched coroutine reach awaitSent()
        assertFalse(reached)    // still waiting
        q.pollFor(linkA)
        yield()                 // let the resumed coroutine continue past awaitSent()
        assertTrue(reached)
    }

    @Test fun `awaitSent returns immediately when entry was already sent`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        q.pollFor(linkA)
        var reached = false
        launch {
            h.awaitSent()       // deferred already completed — should not suspend
            reached = true
        }
        yield()
        assertTrue(reached)
    }

    @Test fun `awaitSent returns immediately via fast path when waiter was present`() = runTest {
        val q = TxQueue()
        val pollDeferred = async { q.pollFor(linkA) }
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        pollDeferred.await()    // fast path completed sentDeferred immediately
        var reached = false
        launch {
            h.awaitSent()
            reached = true
        }
        yield()
        assertTrue(reached)
    }

    // ── cancel ────────────────────────────────────────────────────────────────

    @Test fun `cancel removes the entry so it is never transmitted`() = runTest {
        val q = TxQueue()
        val toCancel = entry(linkA, PacketFlavor.DATA, payload = 1)
        val other    = entry(linkA, PacketFlavor.DATA, payload = 2)
        val h = q.enqueue(toCancel)
        q.enqueue(other)
        assertTrue(h.cancel())
        assertEquals(other, q.pollFor(linkA))
    }

    @Test fun `cancel returns false when entry was already transmitted`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        q.pollFor(linkA)
        assertFalse(h.cancel())
    }

    @Test fun `cancel returns false when called twice`() = runTest {
        val q = TxQueue()
        val other = entry(linkA, PacketFlavor.DATA, payload = 2)
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1))
        q.enqueue(other)
        h.cancel()
        assertFalse(h.cancel())
    }

    // ── replace ───────────────────────────────────────────────────────────────

    @Test fun `replace swaps frame bytes of queued entry`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1))
        assertTrue(h.replace(byteArrayOf(99)))
        val transmitted = q.pollFor(linkA)
        assertEquals(99.toByte(), transmitted.frame[0])
    }

    @Test fun `replace preserves queue position`() = runTest {
        val q = TxQueue()
        val h1 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1, userPriority = 0))
        val h2 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 2, userPriority = 1))
        // h1 has lower userPriority (comes first); replace its bytes
        h1.replace(byteArrayOf(99))
        // h1 should still come first (position unchanged)
        val first  = q.pollFor(linkA)
        val second = q.pollFor(linkA)
        assertEquals(h1.id, first.insertionOrder)
        assertEquals(99.toByte(), first.frame[0])
        assertEquals(h2.id, second.insertionOrder)
    }

    @Test fun `replace returns false when entry was already transmitted`() = runTest {
        val q = TxQueue()
        val h = q.enqueue(entry(linkA, PacketFlavor.DATA))
        q.pollFor(linkA)
        assertFalse(h.replace(byteArrayOf(1)))
    }

    // ── swap ──────────────────────────────────────────────────────────────────

    @Test fun `swap inverts transmission order of two queued entries`() = runTest {
        val q = TxQueue()
        // Different userPriority so ordering is deterministic regardless of enqueuedAt resolution.
        val h1 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1, userPriority = 0))
        val h2 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 2, userPriority = 1))
        // Before swap: h1 first (userPriority 0 < 1)
        assertTrue(h1.swap(h2))
        // After swap: h2 first (now has userPriority 0)
        assertEquals(2.toByte(), q.pollFor(linkA).frame[0])
        assertEquals(1.toByte(), q.pollFor(linkA).frame[0])
    }

    @Test fun `swap returns false when first entry was already transmitted`() = runTest {
        val q = TxQueue()
        val h1 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1, userPriority = 0))
        val h2 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 2, userPriority = 1))
        q.pollFor(linkA)    // transmits h1
        assertFalse(h1.swap(h2))
    }

    @Test fun `swap returns false when second entry was already transmitted`() = runTest {
        val q = TxQueue()
        val h1 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1, userPriority = 1))
        val h2 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 2, userPriority = 0))
        q.pollFor(linkA)    // transmits h2 (lower userPriority = higher priority)
        assertFalse(h1.swap(h2))
    }

    @Test fun `swap does not override controlFirst — control stays ahead of data`() = runTest {
        val q = TxQueue()
        val hData    = q.enqueue(entry(linkA, PacketFlavor.DATA,    payload = 1, userPriority = 0))
        val hControl = q.enqueue(entry(linkA, PacketFlavor.OGM,     payload = 2, userPriority = 1))
        // Swap gives data the control's userPriority and vice versa; controlFirst still wins.
        hData.swap(hControl)
        assertEquals(2.toByte(), q.pollFor(linkA).frame[0])   // OGM still first
        assertEquals(1.toByte(), q.pollFor(linkA).frame[0])
    }

    @Test fun `double swap restores original order`() = runTest {
        val q = TxQueue()
        val h1 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 1, userPriority = 0))
        val h2 = q.enqueue(entry(linkA, PacketFlavor.DATA, payload = 2, userPriority = 1))
        h1.swap(h2)
        h1.swap(h2)    // swap back
        assertEquals(1.toByte(), q.pollFor(linkA).frame[0])
        assertEquals(2.toByte(), q.pollFor(linkA).frame[0])
    }
}
