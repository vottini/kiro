package systems.untangle.kiro

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
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

fun simLink(
    id: String,
    medium: SimMedium,
    interval: Duration = 50.milliseconds,
    bandwidthBps: Long = 100_000_000L,
): Link = object : Link {
    override val id             = id
    override val ogmInterval    = interval
    override val bandwidthBps   = bandwidthBps
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
 *   [KiroRouter.incomingMulticast] and [KiroRouter.incomingData] are
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

    private val nodeById = mutableMapOf<Int, KiroRouter>()
    private data class NodeCfg(val selfId: NodeId, val links: List<Link>)
    private val nodeCfg  = mutableMapOf<KiroRouter, NodeCfg>()

    private fun node(id: UShort, vararg links: Link): KiroRouter {
        val router = KiroRouter()
        nodeCfg[router] = NodeCfg(id, links.toList())
        nodeById[id.toInt()] = router
        return router
    }

    private fun KiroRouter.startIn(scope: CoroutineScope) {
        val cfg = nodeCfg[this]!!
        start(scope, cfg.selfId, cfg.links, staleThreshold = STALE, neighborPurgeMultiplier = 5)
    }

    /**
     * Starts [node] under a child [SupervisorJob] so it can be killed independently
     * by cancelling the returned job without tearing down the whole test scope.
     */
    private fun CoroutineScope.startKillable(node: KiroRouter): CompletableJob {
        val job = SupervisorJob(coroutineContext[Job])
        node.startIn(CoroutineScope(coroutineContext + job))
        return job
    }

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

        a.startIn(this); b.startIn(this); c.startIn(this)
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
        val gid = GroupId(1u)
        a.joinGroup(gid, roots = listOf(a.selfId))
        nodeById[3]!!.joinGroup(gid, roots = listOf(a.selfId))
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

        listOf(a, b, c, d, e, f).forEach { it.startIn(this) }
        delay(OGM_CONV + 100.milliseconds)   // 3-hop OGMs need one extra cycle

        // ── Unicast A → F (3 hops: A→B→D→F) ──
        val uAF = subscribeAndTrigger(f.incomingData) { a.send(6u, "A→F".encodeToByteArray()) }
        assertEquals("A→F", uAF.awaitOrFail(message = "unicast A→F").second.decodeToString())

        // ── Unicast F → C (3 hops: F→D→B→C) ──
        val uFC = subscribeAndTrigger(c.incomingData) { f.send(3u, "F→C".encodeToByteArray()) }
        assertEquals("F→C", uFC.awaitOrFail(message = "unicast F→C").second.decodeToString())

        // ── Multicast: A owns, E and F join ──
        val gid = GroupId(2u)
        a.joinGroup(gid, roots = listOf(a.selfId))
        nodeById[5]!!.joinGroup(gid, roots = listOf(a.selfId))
        nodeById[6]!!.joinGroup(gid, roots = listOf(a.selfId))
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

        listOf(r, a, b, c).forEach { it.startIn(this) }
        delay(OGM_CONV)   // R learns C via A; R learns B direct

        // ── Unicast R→C (2-hop via A) ──
        val uRC = subscribeAndTrigger(c.incomingData) { r.send(4u, "to C".encodeToByteArray()) }
        assertEquals("to C", uRC.awaitOrFail(message = "unicast R→C").second.decodeToString())

        // ── Unicast R→B (1-hop, direct) ──
        val uRB = subscribeAndTrigger(b.incomingData) { r.send(3u, "to B".encodeToByteArray()) }
        assertEquals("to B", uRB.awaitOrFail(message = "unicast R→B").second.decodeToString())

        // ── Multicast: R owns, B and C join ──
        val gid = GroupId(3u)
        r.joinGroup(gid, roots = listOf(r.selfId))
        nodeById[3]!!.joinGroup(gid, roots = listOf(r.selfId))
        nodeById[4]!!.joinGroup(gid, roots = listOf(r.selfId))
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

    // ─── Scenario 4: 10-node ring ─────────────────────────────────────────────
    //
    //   1 ─ 2 ─ 3 ─ 4 ─ 5 ─ 6 ─ 7 ─ 8 ─ 9 ─ 10
    //   └──────────────────────────────────────┘
    //
    //   Each adjacent pair (including 10↔1) shares a dedicated medium.
    //   Diameter = 5 hops; BATMAN picks the shorter of the two arcs.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast 1→4 (3 hops clockwise).
    //   • Unicast 1→7 (4 hops counter-clockwise).
    //   • Multicast: node 1 owns, nodes 4 and 7 join; beacons travel opposing
    //     arcs, so the tree bifurcates at node 1.

    @Test
    fun `scenario 4 — 10-node ring, short-arc and long-arc unicast, bifurcated multicast`() = runBlocking(Dispatchers.Default) {
        // media[i] is the medium between node (i+1) and node ((i+1) % 10 + 1).
        // Node i (1-based): left = media[(i-2+10)%10], right = media[(i-1)%10].
        val media = Array(10) { SimMedium() }
        val nodes = (1..10).map { i ->
            node(i.toUShort(),
                simLink("$i-L", media[(i - 2 + 10) % 10]),
                simLink("$i-R", media[(i - 1) % 10]))
        }
        nodes.forEach { it.startIn(this) }
        delay(OGM_CONV + 250.milliseconds)   // ring diameter = 5 hops; extra propagation time

        // ── Unicast 1→4 (3 hops clockwise: 1→2→3→4) ──
        val u14 = subscribeAndTrigger(nodes[3].incomingData) { nodes[0].send(4u, "1→4".encodeToByteArray()) }
        assertEquals("1→4", u14.awaitOrFail(message = "unicast 1→4").second.decodeToString())

        // ── Unicast 1→7 (4 hops counter-clockwise: 1→10→9→8→7) ──
        val u17 = subscribeAndTrigger(nodes[6].incomingData) { nodes[0].send(7u, "1→7".encodeToByteArray()) }
        assertEquals("1→7", u17.awaitOrFail(message = "unicast 1→7").second.decodeToString())

        // ── Multicast: node 1 owns, nodes 4 and 7 join ──
        // Node 4 beacons: 4→3→2→1 (clockwise arc).
        // Node 7 beacons: 7→8→9→10→1 (counter-clockwise arc).
        // Tree at node 1 has branches in both directions.
        val gid = GroupId(4u)
        nodes[0].joinGroup(gid, roots = listOf(nodes[0].selfId))
        nodeById[4]!!.joinGroup(gid, roots = listOf(nodes[0].selfId))
        nodeById[7]!!.joinGroup(gid, roots = listOf(nodes[0].selfId))
        delay(TREE_BUILD)

        val got4 = CompletableDeferred<MulticastMessage>()
        val got7 = CompletableDeferred<MulticastMessage>()
        launch { nodes[3].incomingMulticast.filter { it.groupId == gid }.first().let { got4.complete(it) } }
        launch { nodes[6].incomingMulticast.filter { it.groupId == gid }.first().let { got7.complete(it) } }
        delay(SUB_DELAY)
        nodes[0].sendMulticast(gid, "ring-cast".encodeToByteArray())

        assertEquals("ring-cast", got4.awaitOrFail(message = "multicast to node 4").payload.decodeToString())
        assertEquals("ring-cast", got7.awaitOrFail(message = "multicast to node 7").payload.decodeToString())

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 5: 15-node complete binary tree ─────────────────────────────
    //
    //                 1
    //            /         \
    //          2               3
    //        /   \           /   \
    //      4       5       6       7
    //     / \     / \     / \     / \
    //    8   9  10  11  12  13  14  15
    //
    //   Exactly one path between any two nodes (tree property).
    //   Leaf-to-leaf diameter = 6 hops (e.g. 8→4→2→1→3→7→15).
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast root→leaf: 1→8 (3 hops).
    //   • Unicast leaf→leaf: 8→15 (6 hops, maximum tree diameter).
    //   • Multicast: root (1) owns, one leaf per subtree of depth-2 nodes
    //     (8, 10, 12, 14) joins; four independent tree branches built.

    @Test
    fun `scenario 5 — 15-node binary tree, root-to-leaf and leaf-to-leaf unicast, 4-leaf multicast`() = runBlocking(Dispatchers.Default) {
        val e12  = SimMedium(); val e13  = SimMedium()
        val e24  = SimMedium(); val e25  = SimMedium()
        val e36  = SimMedium(); val e37  = SimMedium()
        val e48  = SimMedium(); val e49  = SimMedium()
        val e510 = SimMedium(); val e511 = SimMedium()
        val e612 = SimMedium(); val e613 = SimMedium()
        val e714 = SimMedium(); val e715 = SimMedium()

        val n1  = node(1u,  simLink("1-2",  e12),  simLink("1-3",  e13))
        val n2  = node(2u,  simLink("2-1",  e12),  simLink("2-4",  e24),  simLink("2-5",  e25))
        val n3  = node(3u,  simLink("3-1",  e13),  simLink("3-6",  e36),  simLink("3-7",  e37))
        val n4  = node(4u,  simLink("4-2",  e24),  simLink("4-8",  e48),  simLink("4-9",  e49))
        val n5  = node(5u,  simLink("5-2",  e25),  simLink("5-10", e510), simLink("5-11", e511))
        val n6  = node(6u,  simLink("6-3",  e36),  simLink("6-12", e612), simLink("6-13", e613))
        val n7  = node(7u,  simLink("7-3",  e37),  simLink("7-14", e714), simLink("7-15", e715))
        val n8  = node(8u,  simLink("8-4",  e48))
        val n9  = node(9u,  simLink("9-4",  e49))
        val n10 = node(10u, simLink("10-5", e510))
        val n11 = node(11u, simLink("11-5", e511))
        val n12 = node(12u, simLink("12-6", e612))
        val n13 = node(13u, simLink("13-6", e613))
        val n14 = node(14u, simLink("14-7", e714))
        val n15 = node(15u, simLink("15-7", e715))

        listOf(n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15)
            .forEach { it.startIn(this) }
        delay(OGM_CONV + 300.milliseconds)   // 6-hop leaf-to-leaf diameter; extra propagation time

        // ── Unicast root→leaf: 1→8 (3 hops: 1→2→4→8) ──
        val u18 = subscribeAndTrigger(n8.incomingData) { n1.send(8u, "1→8".encodeToByteArray()) }
        assertEquals("1→8", u18.awaitOrFail(message = "unicast 1→8").second.decodeToString())

        // ── Unicast leaf→leaf: 8→15 (6 hops: 8→4→2→1→3→7→15) ──
        val u815 = subscribeAndTrigger(n15.incomingData) { n8.send(15u, "8→15".encodeToByteArray()) }
        assertEquals("8→15", u815.awaitOrFail(message = "unicast 8→15").second.decodeToString())

        // ── Multicast: root owns, one leaf per depth-2 subtree joins ──
        val gid = GroupId(5u)
        n1.joinGroup(gid, roots = listOf(n1.selfId))
        nodeById[8]!!.joinGroup(gid, roots = listOf(n1.selfId))
        nodeById[10]!!.joinGroup(gid, roots = listOf(n1.selfId))
        nodeById[12]!!.joinGroup(gid, roots = listOf(n1.selfId))
        nodeById[14]!!.joinGroup(gid, roots = listOf(n1.selfId))
        delay(TREE_BUILD)

        val got8  = CompletableDeferred<MulticastMessage>()
        val got10 = CompletableDeferred<MulticastMessage>()
        val got12 = CompletableDeferred<MulticastMessage>()
        val got14 = CompletableDeferred<MulticastMessage>()
        launch { n8.incomingMulticast.filter  { it.groupId == gid }.first().let { got8.complete(it)  } }
        launch { n10.incomingMulticast.filter { it.groupId == gid }.first().let { got10.complete(it) } }
        launch { n12.incomingMulticast.filter { it.groupId == gid }.first().let { got12.complete(it) } }
        launch { n14.incomingMulticast.filter { it.groupId == gid }.first().let { got14.complete(it) } }
        delay(SUB_DELAY)
        n1.sendMulticast(gid, "tree-cast".encodeToByteArray())

        assertEquals("tree-cast", got8.awaitOrFail(message  = "multicast to node 8").payload.decodeToString())
        assertEquals("tree-cast", got10.awaitOrFail(message = "multicast to node 10").payload.decodeToString())
        assertEquals("tree-cast", got12.awaitOrFail(message = "multicast to node 12").payload.decodeToString())
        assertEquals("tree-cast", got14.awaitOrFail(message = "multicast to node 14").payload.decodeToString())

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 6: 4×4 grid mesh (16 nodes) ────────────────────────────────
    //
    //    1 ─ 2 ─ 3 ─ 4
    //    │   │   │   │
    //    5 ─ 6 ─ 7 ─ 8
    //    │   │   │   │
    //    9 ─10 ─11 ─12
    //    │   │   │   │
    //   13 ─14 ─15 ─16
    //
    //   Nodes are connected to their horizontal and vertical neighbours.
    //   Diameter = 6 hops (corner-to-corner). Many shortest paths exist.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast corner-to-corner: 1→16 and 4→13 (6 hops each).
    //   • Multicast: node 6 (upper-centre) owns, all four corners join;
    //     beacons arrive 3–4 hops away, building a tree that fans out to
    //     cover all four quadrants.

    @Test
    fun `scenario 6 — 4×4 grid mesh, corner-to-corner unicast and 4-corner multicast`() = runBlocking(Dispatchers.Default) {
        val N = 4
        // hMedia[r][c] is the medium between node(r,c) and node(r,c+1).
        val hMedia = Array(N) { Array(N - 1) { SimMedium() } }
        // vMedia[r][c] is the medium between node(r,c) and node(r+1,c).
        val vMedia = Array(N - 1) { Array(N) { SimMedium() } }

        fun nodeId(r: Int, c: Int): UShort = (r * N + c + 1).toUShort()

        val grid = Array(N) { r ->
            Array(N) { c ->
                val links = mutableListOf<Link>()
                if (c > 0)   links += simLink("${nodeId(r,c)}-W", hMedia[r][c - 1])
                if (c < N-1) links += simLink("${nodeId(r,c)}-E", hMedia[r][c])
                if (r > 0)   links += simLink("${nodeId(r,c)}-N", vMedia[r - 1][c])
                if (r < N-1) links += simLink("${nodeId(r,c)}-S", vMedia[r][c])
                node(nodeId(r, c), *links.toTypedArray())
            }
        }
        grid.forEach { row -> row.forEach { it.startIn(this) } }
        delay(OGM_CONV + 500.milliseconds)   // 6-hop diameter; generous convergence window

        fun n(id: Int) = grid[(id - 1) / N][(id - 1) % N]

        // ── Unicast 1→16 (6 hops: e.g. 1→2→3→4→8→12→16) ──
        val u1_16 = subscribeAndTrigger(n(16).incomingData) { n(1).send(16u, "1→16".encodeToByteArray()) }
        assertEquals("1→16", u1_16.awaitOrFail(message = "unicast 1→16").second.decodeToString())

        // ── Unicast 4→13 (6 hops: e.g. 4→3→2→1→5→9→13) ──
        val u4_13 = subscribeAndTrigger(n(13).incomingData) { n(4).send(13u, "4→13".encodeToByteArray()) }
        assertEquals("4→13", u4_13.awaitOrFail(message = "unicast 4→13").second.decodeToString())

        // ── Multicast: node 6 owns, four corners join ──
        val gid = GroupId(6u)
        n(6).joinGroup(gid, roots = listOf(n(6).selfId))
        nodeById[1]!!.joinGroup(gid, roots = listOf(n(6).selfId))
        nodeById[4]!!.joinGroup(gid, roots = listOf(n(6).selfId))
        nodeById[13]!!.joinGroup(gid, roots = listOf(n(6).selfId))
        nodeById[16]!!.joinGroup(gid, roots = listOf(n(6).selfId))
        delay(TREE_BUILD)

        val corners = mapOf(1 to CompletableDeferred(), 4 to CompletableDeferred<MulticastMessage>(),
                            13 to CompletableDeferred(), 16 to CompletableDeferred<MulticastMessage>())
        corners.forEach { (id, def) ->
            launch { n(id).incomingMulticast.filter { it.groupId == gid }.first().let { def.complete(it) } }
        }
        delay(SUB_DELAY)
        n(6).sendMulticast(gid, "grid-cast".encodeToByteArray())

        corners.forEach { (id, def) ->
            assertEquals("grid-cast", def.awaitOrFail(message = "multicast to node $id").payload.decodeToString())
        }

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 7: redundant relay dies — unicast reroutes ─────────────────
    //
    //        B(2)
    //       /    \
    //   A(1)      D(4)      ← diamond: two equal-cost paths A→D
    //       \    /
    //        C(3)
    //
    //   A connects to B (mAB) and C (mAC); D connects to B (mBD) and C (mCD).
    //   Both A→B→D and A→C→D are 2 hops with equal TTL. BATMAN picks whichever
    //   relay's OGM most recently arrived. After B is killed, only C's relayed
    //   OGMs reach A, so A's route to D naturally switches to C.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast A→D works before B dies.
    //   • Unicast A→D still works after B dies (rerouted via C).

    @Test
    fun `scenario 7 — redundant relay dies, unicast reroutes via surviving path`() = runBlocking(Dispatchers.Default) {
        val mAB = SimMedium(); val mAC = SimMedium()
        val mBD = SimMedium(); val mCD = SimMedium()

        val a = node(1u, simLink("A-B", mAB), simLink("A-C", mAC))
        val b = node(2u, simLink("B-A", mAB), simLink("B-D", mBD))
        val c = node(3u, simLink("C-A", mAC), simLink("C-D", mCD))
        val d = node(4u, simLink("D-B", mBD), simLink("D-C", mCD))

        val bJob = startKillable(b)
        listOf(a, c, d).forEach { it.startIn(this) }
        delay(OGM_CONV)

        // ── Baseline: A→D works (2 hops, via B or C) ──
        val pre = subscribeAndTrigger(d.incomingData) { a.send(4u, "before".encodeToByteArray()) }
        assertEquals("before", pre.awaitOrFail(message = "unicast A→D before B dies").second.decodeToString())

        // ── Kill B ──
        bJob.cancel()
        delay(OGM_CONV)   // C's OGMs take over A's route to D; one full convergence cycle

        // ── A→D must reach via C (A→C→D) ──
        val post = subscribeAndTrigger(d.incomingData) { a.send(4u, "after".encodeToByteArray()) }
        assertEquals("after", post.awaitOrFail(message = "unicast A→D after B dies, rerouted via C").second.decodeToString())

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 8: sole bridge dies — network partitions ───────────────────
    //
    //   {A(1), C(3)} ── mL ── B(2) ── mR ── {D(4), E(5)}
    //
    //   B is the only node bridging the left and right broadcast domains.
    //   After it dies, left and right can no longer exchange any frames.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Unicast A→E works before B dies.
    //   • Unicast A→E fails (no delivery within timeout) after B dies.

    @Test
    fun `scenario 8 — sole bridge dies, network partitions`() = runBlocking(Dispatchers.Default) {
        val mL = SimMedium(); val mR = SimMedium()
        val a = node(1u, simLink("A-L", mL))
        val b = node(2u, simLink("B-L", mL), simLink("B-R", mR))
        val c = node(3u, simLink("C-L", mL))
        val d = node(4u, simLink("D-R", mR))
        val e = node(5u, simLink("E-R", mR))

        val bJob = startKillable(b)
        listOf(a, c, d, e).forEach { it.startIn(this) }
        delay(OGM_CONV + 100.milliseconds)

        // ── Baseline: A→E works through B ──
        val pre = subscribeAndTrigger(e.incomingData) { a.send(5u, "before".encodeToByteArray()) }
        assertEquals("before", pre.awaitOrFail(message = "unicast A→E before bridge dies").second.decodeToString())

        // ── Kill sole bridge B ──
        bJob.cancel()
        delay(OGM_CONV)   // A's entries for D and E expire; no alternative path exists

        // ── A→E must now fail — left and right clusters are partitioned ──
        val received = withTimeoutOrNull(1.seconds) {
            coroutineScope {
                val def = CompletableDeferred<Pair<NodeId, ByteArray>>()
                launch { e.incomingData.first().let { def.complete(it) } }
                delay(SUB_DELAY)
                a.send(5u, "lost".encodeToByteArray())
                def.await()
            }
        }
        assertNull(received, "A→E should be unreachable after sole bridge dies")

        coroutineContext.cancelChildren()
    }

    // ─── Scenario 9: multicast relay dies — spanning tree heals ──────────────
    //
    //        R1(2)
    //       /     \
    //   S(1)       M(4)      ← S owns group; M is the sole member
    //       \     /
    //        R2(3)
    //
    //   M→S beacons travel via R1 or R2 (equal 2-hop cost). After R1 is killed,
    //   M's route to S switches to R2, M's beacons rebuild the tree via R2, and
    //   S's multicast reaches M again — demonstrating end-to-end tree healing.
    //
    //   Assertions
    //   ─────────────────────────────────────────────────────────────────────
    //   • Multicast S→M works before R1 dies.
    //   • Multicast S→M still works after R1 dies (tree rebuilt via R2).

    @Test
    fun `scenario 9 — multicast relay dies, spanning tree heals via redundant path`() = runBlocking(Dispatchers.Default) {
        val mSR1 = SimMedium(); val mSR2 = SimMedium()
        val mR1M = SimMedium(); val mR2M = SimMedium()

        val s  = node(1u, simLink("S-R1",  mSR1), simLink("S-R2",  mSR2))
        val r1 = node(2u, simLink("R1-S",  mSR1), simLink("R1-M",  mR1M))
        val r2 = node(3u, simLink("R2-S",  mSR2), simLink("R2-M",  mR2M))
        val m  = node(4u, simLink("M-R1",  mR1M), simLink("M-R2",  mR2M))

        val r1Job = startKillable(r1)
        listOf(s, r2, m).forEach { it.startIn(this) }
        delay(OGM_CONV)

        val gid = GroupId(9u)
        s.joinGroup(gid, roots = listOf(s.selfId))
        nodeById[4]!!.joinGroup(gid, roots = listOf(s.selfId))
        delay(TREE_BUILD)   // M's beacons build S's tree via R1 or R2

        // ── Baseline: multicast S→M works ──
        val pre = subscribeAndTrigger(m.incomingMulticast, { it.groupId == gid }) {
            s.sendMulticast(gid, "before".encodeToByteArray())
        }
        assertEquals("before", pre.awaitOrFail(message = "multicast S→M before R1 dies").payload.decodeToString())

        // ── Kill R1 ──
        r1Job.cancel()
        // M re-routes to S via R2; new beacons rebuild the tree through R2.
        // Healing needs: one OGM_CONV for M's routing table to switch to R2,
        // plus one beacon cycle (BEACON) for the new tree branch to reach S.
        delay(OGM_CONV + BEACON)

        // ── Multicast S→M must still reach M via R2 ──
        val post = subscribeAndTrigger(m.incomingMulticast, { it.groupId == gid }) {
            s.sendMulticast(gid, "after".encodeToByteArray())
        }
        assertEquals("after", post.awaitOrFail(message = "multicast S→M after R1 dies, tree via R2").payload.decodeToString())

        coroutineContext.cancelChildren()
    }
}
