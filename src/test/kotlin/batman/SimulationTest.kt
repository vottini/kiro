package batman

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// ─── Virtual network primitives ───────────────────────────────────────────────

/**
 * In-memory broadcast domain. Every [SimLink] that calls [emit] causes all
 * collectors on this medium to receive the frame — exactly like a shared radio
 * channel. DROP_OLDEST prevents slow consumers from back-pressuring the sender.
 */
class SimMedium {
    private val _flow = MutableSharedFlow<ByteArray>(
        extraBufferCapacity = 256,
        onBufferOverflow    = BufferOverflow.DROP_OLDEST
    )
    val flow: SharedFlow<ByteArray> get() = _flow
    suspend fun emit(frame: ByteArray) { _flow.emit(frame) }
}

fun simLink(id: String, medium: SimMedium, interval: Duration = 50.milliseconds): Link =
    object : Link {
        override val id          = id
        override val ogmInterval = interval
        override suspend fun broadcast(frame: ByteArray) = medium.emit(frame)
        override val frames: Flow<ByteArray>             = medium.flow
    }

// ─── Simulation test suite ────────────────────────────────────────────────────

/**
 * End-to-end simulations running real coroutines on in-memory links.
 *
 * Timing constants:
 *   OGM_CONV   = 400 ms → 8 OGM cycles at 50 ms; routes stabilise.
 *   STALE      = 1.2 s  → beacon interval = 400 ms (staleThreshold / 3).
 *   TREE_BUILD = 1.6 s  → 4 beacon cycles; spanning tree fully populated.
 *   SUB_DELAY  = 100 ms → time given to a subscriber coroutine to register
 *                         on the hot SharedFlow before the trigger fires.
 *
 * Hot-flow subscriber race:
 *   [BatmanRouter.incomingMulticast] and [BatmanRouter.incomingData] are
 *   zero-buffer SharedFlows; an emission with no active subscriber is dropped.
 *   On [Dispatchers.Default] (multi-threaded), [Flow.onSubscription] does not
 *   reliably solve this because the action may fire on a second thread before
 *   [SharedFlow.collect] has registered the subscriber on the first thread.
 *
 *   The fix used here: subscribe via [launch], then [delay] 100 ms to guarantee
 *   the subscriber coroutine has started and registered, then fire the send.
 *   100 ms >> typical coroutine startup time (< 1 ms), making this reliable
 *   without adding meaningful overhead compared to the 400–1600 ms delays
 *   already present for OGM/beacon convergence.
 */
class SimulationTest {

    private val STALE      = 1.2.seconds
    private val BEACON     = STALE / 3      // 400 ms
    private val OGM_CONV   = 400.milliseconds
    private val TREE_BUILD = BEACON * 4     // 1600 ms
    private val SUB_DELAY  = 100.milliseconds

    private fun node(id: UShort, vararg links: Link) =
        BatmanRouter(selfId = id.toUShort(), links = links.toList(), staleThreshold = STALE)

    /** Subscribes [flow], waits [SUB_DELAY] for the subscriber to register, then runs [trigger]. */
    private suspend fun <T> CoroutineScope.subscribeAndTrigger(
        flow: Flow<T>,
        predicate: (T) -> Boolean = { true },
        trigger: suspend () -> Unit
    ): CompletableDeferred<T> {
        val result = CompletableDeferred<T>()
        launch { flow.filter(predicate).first().let { result.complete(it) } }
        delay(SUB_DELAY)
        trigger()
        return result
    }

    private suspend fun <T> CompletableDeferred<T>.awaitOrFail(
        timeout: Duration = 5.seconds,
        message: String = "event not received"
    ): T = withTimeoutOrNull(timeout) { await() } ?: error("$message within $timeout")

    // ─── Scenario 1: 2-hop linear chain ──────────────────────────────────────
    //
    //   A ──m1── B ──m2── C
    //
    //   A and C cannot hear each other directly. B is the sole relay node.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • OGM convergence: after 400 ms both A and C have routes through B.
    //   • Unicast A→C and reverse C→A, both delivered with correct srcId.
    //   • Multicast: A owns, C joins, B relays without being a group member.

    @Test
    fun `scenario 1 — 2-hop line, unicast and multicast`() = runBlocking(Dispatchers.Default) {
        val m1 = SimMedium(); val m2 = SimMedium()
        val a  = node(1u, simLink("A-1", m1))
        val b  = node(2u, simLink("B-1", m1), simLink("B-2", m2))
        val c  = node(3u, simLink("C-2", m2))

        a.start(this); b.start(this); c.start(this)
        delay(OGM_CONV)   // A↔B↔C routes established

        // ── Unicast A → C ──
        val u1 = subscribeAndTrigger(c.incomingData) { a.send(3u, "hello C".encodeToByteArray()) }
        val (srcId, uPayload) = u1.awaitOrFail(message = "unicast A→C")
        assertEquals(1u.toUShort(), srcId)
        assertEquals("hello C", uPayload.decodeToString())

        // ── Unicast C → A (reverse) ──
        val u2 = subscribeAndTrigger(a.incomingData) { c.send(1u, "hello A".encodeToByteArray()) }
        assertEquals("hello A", u2.awaitOrFail(message = "unicast C→A").second.decodeToString())

        // ── Multicast: A owns, C joins; B relays without being a member ──
        val gid = a.createGroup()
        a.invite(gid, 3u)
        delay(TREE_BUILD)   // C beacons: C→B→A; tree built on B and A

        val m1rcv = subscribeAndTrigger(c.incomingMulticast, { it.groupId == gid }) {
            a.sendMulticast(gid, "multicast!".encodeToByteArray())
        }
        assertEquals("multicast!", m1rcv.awaitOrFail(message = "multicast A→C").payload.decodeToString())

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 2: two cliques bridged by a single link ────────────────────
    //
    //   Clique1 {A B C} on m1                m2 {D E F} Clique2
    //                      B ──── m3 ──── D
    //
    //   Within each clique every node hears every other node.
    //   B↔D is the sole bridge link; paths between cliques are 3 hops.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Cross-clique unicast: A→F (A→B→D→F) and F→C (F→D→B→C).
    //   • Multicast: A owns, E and F join; beacon path 3 hops E/F→D→B→A.
    //     Fan-out: multicast reaches both E and F via A→B→D→{E,F}.

    @Test
    fun `scenario 2 — clique-bridge-clique, cross-clique unicast and multicast`() = runBlocking(Dispatchers.Default) {
        val m1 = SimMedium(); val m2 = SimMedium(); val m3 = SimMedium()
        val a  = node(1u, simLink("A-1", m1))
        val b  = node(2u, simLink("B-1", m1), simLink("B-3", m3))
        val c  = node(3u, simLink("C-1", m1))
        val d  = node(4u, simLink("D-3", m3), simLink("D-2", m2))
        val e  = node(5u, simLink("E-2", m2))
        val f  = node(6u, simLink("F-2", m2))

        listOf(a, b, c, d, e, f).forEach { it.start(this) }
        delay(OGM_CONV + 100.milliseconds)   // 3-hop OGMs need one extra cycle

        // ── Unicast A → F (3 hops: A→B→D→F) ──
        val uAF = subscribeAndTrigger(f.incomingData) { a.send(6u, "A→F".encodeToByteArray()) }
        assertEquals("A→F", uAF.awaitOrFail(message = "unicast A→F").second.decodeToString())

        // ── Unicast F → C (3 hops: F→D→B→C) ──
        val uFC = subscribeAndTrigger(c.incomingData) { f.send(3u, "F→C".encodeToByteArray()) }
        assertEquals("F→C", uFC.awaitOrFail(message = "unicast F→C").second.decodeToString())

        // ── Multicast: A owns, E and F join ──
        val gid = a.createGroup()
        a.invite(gid, 5u); a.invite(gid, 6u)
        delay(TREE_BUILD)   // 3-hop beacons E/F→D→B→A build the tree

        // Subscribe both receivers before sending, with a single SUB_DELAY shared.
        val gotE = CompletableDeferred<MulticastMessage>()
        val gotF = CompletableDeferred<MulticastMessage>()
        launch { e.incomingMulticast.filter { it.groupId == gid }.first().let { gotE.complete(it) } }
        launch { f.incomingMulticast.filter { it.groupId == gid }.first().let { gotF.complete(it) } }
        delay(SUB_DELAY)   // both subscribers active before send
        a.sendMulticast(gid, "cross-clique".encodeToByteArray())

        assertEquals("cross-clique", gotE.awaitOrFail(message = "multicast to E").payload.decodeToString())
        assertEquals("cross-clique", gotF.awaitOrFail(message = "multicast to F").payload.decodeToString())

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 3: tree with redundant R-A radio interfaces ────────────────
    //
    //           R
    //         / | \
    //     fast slow  B        R-A: two parallel links (30 ms and 150 ms OGM)
    //         \ |             BATMAN prefers the fast link (OGMs arrive first).
    //           A ──mAC── C   R-B and A-C: one link each (100 ms).
    //
    //   R and A each have two radio interfaces on mFast (30 ms OGM) and mSlow
    //   (150 ms OGM). The fast-interval link delivers OGMs first, so it wins
    //   the neighbor-table race and becomes R's preferred path to A and vice-versa.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast R→C (2-hop via A using the fast link) and R→B (direct).
    //   • Multicast: R owns, B and C join. Beacons C→A→R and B→R build tree.
    //     Both leaves B and C receive the multicast.

    @Test
    fun `scenario 3 — tree with redundant R-A links, multicast to leaves`() = runBlocking(Dispatchers.Default) {
        val mFast = SimMedium(); val mSlow = SimMedium()
        val mRB   = SimMedium(); val mAC   = SimMedium()

        val r = node(1u,
            simLink("R-fast", mFast, 30.milliseconds),
            simLink("R-slow", mSlow, 150.milliseconds),
            simLink("R-B",    mRB,   100.milliseconds))
        val a = node(2u,
            simLink("A-fast", mFast, 30.milliseconds),
            simLink("A-slow", mSlow, 150.milliseconds),
            simLink("A-C",    mAC,   100.milliseconds))
        val b = node(3u, simLink("B-R", mRB, 100.milliseconds))
        val c = node(4u, simLink("C-A", mAC, 100.milliseconds))

        listOf(r, a, b, c).forEach { it.start(this) }
        delay(OGM_CONV)   // R learns C via A; R learns B direct

        // ── Unicast R→C (2-hop via A) ──
        val uRC = subscribeAndTrigger(c.incomingData) { r.send(4u, "to C".encodeToByteArray()) }
        assertEquals("to C", uRC.awaitOrFail(message = "unicast R→C").second.decodeToString())

        // ── Unicast R→B (1-hop, direct) ──
        val uRB = subscribeAndTrigger(b.incomingData) { r.send(3u, "to B".encodeToByteArray()) }
        assertEquals("to B", uRB.awaitOrFail(message = "unicast R→B").second.decodeToString())

        // ── Multicast: R owns, B and C join ──
        val gid = r.createGroup()
        r.invite(gid, 3u); r.invite(gid, 4u)
        delay(TREE_BUILD)   // C beacons C→A→R; B beacons B→R; tree fully built

        val gotB = CompletableDeferred<MulticastMessage>()
        val gotC = CompletableDeferred<MulticastMessage>()
        launch { b.incomingMulticast.filter { it.groupId == gid }.first().let { gotB.complete(it) } }
        launch { c.incomingMulticast.filter { it.groupId == gid }.first().let { gotC.complete(it) } }
        delay(SUB_DELAY)   // both subscribers active before send
        r.sendMulticast(gid, "to leaves".encodeToByteArray())

        assertEquals("to leaves", gotB.awaitOrFail(message = "multicast to B").payload.decodeToString())
        assertEquals("to leaves", gotC.awaitOrFail(message = "multicast to C").payload.decodeToString())

        coroutineContext.cancelChildren()
    }
}
