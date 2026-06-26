package systems.untangle.kiro

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Maximum TTL assigned to newly created OGMs and data frames. */
const val MAX_TTL: UByte = 15u

/**
 * Payload delivered to [KiroRouter.incomingMulticast] collectors when a
 * [Frame.MulticastFrame] is received and this node is a member of the target group.
 */
data class MulticastMessage(val srcId: NodeId, val groupId: GroupId, val payload: ByteArray)

/**
 * Core BATMAN (Better Approach To Mobile Ad-hoc Networking) mesh router,
 * implemented entirely at the application layer.
 *
 * ## Routing
 *
 * Each node periodically broadcasts OGMs (Originator Messages) on every [Link].
 * Neighbouring nodes relay these OGMs with a decremented TTL. By tracking which
 * neighbour delivered each originator's OGM with the highest remaining TTL, every
 * node builds a next-hop routing table ([neighborTable]) without ever computing
 * full paths. Data frames are forwarded hop-by-hop using this table.
 *
 * ## OGM relay with jitter suppression
 *
 * On a shared broadcast medium all nodes hear the same OGM simultaneously. Without
 * suppression, all N nodes would relay it, causing N−1 redundant transmissions that
 * waste precious band-limited capacity. To mitigate this:
 *
 *  1. On first hearing an OGM, each node waits a random jitter before relaying.
 *  2. During the jitter window, the node counts how many times it hears the same
 *     OGM (from other nodes' relays). This count is stored in [pendingRelays].
 *  3. When the jitter timer fires, the relay probability is 1/hearingCount.
 *     The first node to act relays with 100% probability; subsequent nodes, having
 *     already heard one relay, have progressively lower probability. In a dense
 *     subnet the expected total relays converges to ~2 regardless of N.
 *
 * ## Multicast groups
 *
 * Members join by calling [joinGroup] directly (membership is managed at the
 * application layer). Each member begins sending periodic [Frame.BeaconFrame]s
 * toward the group roots. Relay nodes record both legs of each beacon's path in
 * [multicastTree], building a spanning tree. Any group member can then call
 * [sendMulticast] to reach all members via tree-based forwarding rather than
 * network-wide flooding.
 *
 * ## Concurrency model
 *
 * All per-link loops (receive, OGM emit, TX drain) are launched as coroutines
 * within the [CoroutineScope] passed to [start]. State shared across coroutines
 * ([neighborTable], [pendingRelays], [multicastTree]) uses [ConcurrentHashMap]
 * and [AtomicInteger] to avoid explicit locking.
 *
 */
class KiroRouter {

    var selfId: NodeId = 0u
        private set
    var txQueue: TxQueue = TxQueue()
        private set
    var staleThreshold: Duration = 90.seconds
        private set
    var neighborPurgeMultiplier: Int = 3
        private set

    /** Live set of active links. Returns a snapshot; safe to call from any thread. */
    val links: List<Link> get() = linksMap.values.toList()

    private var linksMap = ConcurrentHashMap<String, Link>()

    /** Per-link SupervisorJob: cancelling one stops only that link's three coroutines. */
    private var linkJobs = ConcurrentHashMap<String, Job>()
    // --- Unicast routing state ---

    /** Next-hop routing table: originator NodeId → best known route entry. */
    private var neighborTable = ConcurrentHashMap<NodeId, NeighborEntry>()

    /** Deduplicates OGMs so each (originator, seq) is processed and relayed at most once. */
    private var seenOgms = SeenWindowCache()

    /** Global OGM sequence counter shared across all links on this node. */
    private var ogmSeq = AtomicInteger(0)

    /**
     * Tracks how many times each pending OGM relay has been heard during its jitter window.
     * Key: (originatorId, seqNum). Value: hearing count (starts at 1 on first receipt).
     * Entries are created when the first copy of an OGM arrives and removed when
     * the jitter timer fires and the relay decision is made.
     */
    private var pendingRelays = ConcurrentHashMap<Pair<NodeId, UShort>, AtomicInteger>()

    // --- Multicast state ---

    /** Spanning tree of links per group, built from beacon relay observations. */
    private var multicastTree = MulticastTree()

    /** Deduplicates multicast frames so each (srcId, seqNum) is delivered/relayed once. */
    private var seenMulticasts = SeenWindowCache()

    /** Per-node multicast sequence counter for frames originated by this node. */
    private var multicastSeq = AtomicInteger(0)

    // --- Group membership ---

    /** Set of [GroupId]s this node belongs to. */
    private var localGroups = ConcurrentHashMap.newKeySet<GroupId>()

    /**
     * Ordered root candidates per group, stored when this node joins a group.
     * [beaconLoop] sends beacons toward the first root that has a known route in
     * [neighborTable].
     */
    private var groupRoots = ConcurrentHashMap<GroupId, List<NodeId>>()

    // --- Public flows ---

    private val _incomingData = MutableSharedFlow<Pair<NodeId, ByteArray>>()
    /** Emits (srcId, payload) whenever a unicast [Frame.DataFrame] addressed to this node arrives. */
    val incomingData: SharedFlow<Pair<NodeId, ByteArray>> = _incomingData

    private val _incomingMulticast = MutableSharedFlow<MulticastMessage>()
    /** Emits a [MulticastMessage] whenever a group multicast arrives and this node is a member. */
    val incomingMulticast: SharedFlow<MulticastMessage> = _incomingMulticast

    private val _routes = MutableStateFlow<Map<NodeId, NeighborEntry>>(emptyMap())
    /**
     * Live view of the routing table. Emits a new snapshot whenever a route is added,
     * updated, or evicted. [value] gives the current snapshot without subscribing.
     *
     * The flow is reset to an empty map on each [start] call so collectors see a clean
     * slate after a restart without needing to resubscribe.
     */
    val routes: StateFlow<Map<NodeId, NeighborEntry>> = _routes.asStateFlow()

    private var scope: CoroutineScope? = null

    @Volatile private var silent = false

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Configures and starts the router. Safe to call again after [stop] — all
     * internal state is reset so the node starts fresh with no stale routes or groups.
     *
     * @param scope Parent scope whose cancellation also stops the router.
     * @param selfId This node's unique identifier within the mesh.
     * @param links Initial radio interfaces. More can be added later with [addLink];
     *   individual links can be removed with [removeLink].
     * @param txQueue Central transmit queue; injectable for testing.
     * @param staleThreshold How long without a beacon refresh before a multicast tree
     *   branch is considered dead and evicted. Should be at least 3× the beacon interval.
     * @param neighborPurgeMultiplier How many missed OGM cycles before a neighbour table
     *   entry is considered unreachable and removed.
     */
    fun start(
        scope: CoroutineScope,
        selfId: NodeId,
        links: List<Link>,
        txQueue: TxQueue = TxQueue(),
        staleThreshold: Duration = 90.seconds,
        neighborPurgeMultiplier: Int = 3
    ) {
        stop()  // cancel any previous run before reconfiguring

        this.selfId                  = selfId
        this.txQueue                 = txQueue
        this.staleThreshold          = staleThreshold
        this.neighborPurgeMultiplier = neighborPurgeMultiplier
        this.silent                  = false

        _routes.value   = emptyMap()
        neighborTable   = ConcurrentHashMap()
        seenOgms        = SeenWindowCache()
        ogmSeq          = AtomicInteger(0)
        pendingRelays   = ConcurrentHashMap()
        multicastTree   = MulticastTree()
        seenMulticasts  = SeenWindowCache()
        multicastSeq    = AtomicInteger(0)
        localGroups     = ConcurrentHashMap.newKeySet()
        groupRoots      = ConcurrentHashMap()
        linksMap        = ConcurrentHashMap()
        linkJobs        = ConcurrentHashMap()

        val job = Job(scope.coroutineContext[Job])
        val routerScope = CoroutineScope(scope.coroutineContext + job)
        this.scope = routerScope

        links.forEach { startLink(it) }
        routerScope.launch { staleEvictionLoop() }      // prunes dead multicast tree branches
        routerScope.launch { neighborPurgeLoop() }      // evicts unreachable neighbour table entries
    }

    /**
     * Adds [link] to the running router and immediately starts its receive, OGM,
     * and TX coroutines. Safe to call while the router is running. Has no effect
     * if a link with the same [Link.id] is already registered (the existing link
     * and its coroutines are left unchanged).
     *
     * Does nothing if the router has not been started.
     */
    fun addLink(link: Link) {
        if (linksMap.putIfAbsent(link.id, link) == null) startLink(link)
    }

    /**
     * Removes the link with [linkId] from the running router.
     *
     * The link's three coroutines (receive, OGM, TX) are cancelled immediately.
     * Any routing table entries that were learned via this link are also evicted
     * right away rather than waiting for the normal purge timeout, so the router
     * does not keep forwarding traffic down a dead interface.
     *
     * Returns `true` if the link existed and was removed, `false` if not found.
     * Does nothing if the router has not been started.
     */
    fun removeLink(linkId: String): Boolean {
        linksMap.remove(linkId) ?: return false
        linkJobs.remove(linkId)?.cancel()
        val removed = neighborTable.entries.removeIf { (_, e) -> e.link.id == linkId }
        if (removed) _routes.value = neighborTable.toMap()
        return true
    }

    /** Launches the three per-link coroutines under their own [SupervisorJob]. */
    private fun startLink(link: Link) {
        val s = scope ?: return
        linksMap[link.id] = link
        val job = SupervisorJob(s.coroutineContext[Job])
        linkJobs[link.id] = job
        val linkScope = CoroutineScope(s.coroutineContext + job)
        linkScope.launch { receiveLoop(link) }
        linkScope.launch { ogmLoop(link) }
        linkScope.launch { txLoop(link) }
    }

    /**
     * Cancels all protocol loops and resets the router to an idle state.
     * The [incomingData] and [incomingMulticast] flows remain valid; collectors
     * do not need to resubscribe before the next [start] call.
     */
    fun stop() {
        scope?.coroutineContext?.get(Job)?.cancel()
        scope = null
    }

    // -------------------------------------------------------------------------
    // Public API — Unicast
    // -------------------------------------------------------------------------

    /**
     * Sends [payload] to [dstId] via the best known next-hop route.
     * Silently drops the frame if no route to [dstId] exists yet.
     */
    suspend fun send(dstId: NodeId, payload: ByteArray) {
        if (silent) return
        val route = neighborTable[dstId] ?: return
        val frame = Frame.DataFrame(
            nextHop = route.nextHop,
            srcId   = selfId,
            dstId   = dstId,
            ttl     = MAX_TTL,
            payload = payload
        )
        txQueue.enqueue(TxEntry(encode(frame), setOf(route.link), PacketFlavor.DATA))
    }

    // -------------------------------------------------------------------------
    // Public API — Group membership
    // -------------------------------------------------------------------------

    /**
     * Registers this node as a member of [gid] and starts the beacon loop that
     * maintains this node's branch in the multicast spanning tree.
     *
     * [roots] is stored in [groupRoots] and consulted by [beaconLoop]: the first
     * root with a known route becomes the [Frame.BeaconFrame.activeRoot], keeping
     * the tree alive. If [selfId] is in [roots], this node IS the root — beacons
     * flow toward it, not from it, so no beacon loop is launched. If [roots] is
     * empty, there is no destination to beacon toward and no loop is launched.
     *
     * [beaconInterval] controls how often beacons are sent. It should be shorter
     * than [staleThreshold] (default: staleThreshold/3) so the tree branch does
     * not expire between refreshes. Slower links may warrant longer intervals.
     */
    fun joinGroup(
        gid: GroupId,
        roots: List<NodeId> = emptyList(),
        beaconInterval: Duration = staleThreshold / 3
    ) {
        localGroups.add(gid)
        if (roots.isNotEmpty()) groupRoots[gid] = roots
        if (roots.isNotEmpty() && selfId !in roots) {
            scope?.launch { beaconLoop(gid, beaconInterval) }
        }
    }

    /**
     * Removes this node from group [gid].
     *
     * The beacon loop for [gid] exits on its next iteration (it checks [localGroups]
     * each cycle), so the upstream branch naturally expires on other nodes within
     * one [staleThreshold] period without requiring any explicit leave message.
     * The local tree state is cleared immediately so this node stops relaying
     * multicasts for the group right away.
     *
     * Returns `true` if this node was a member, `false` if it was not.
     */
    fun leaveGroup(gid: GroupId): Boolean {
        val wasMember = localGroups.remove(gid)
        if (!wasMember) return false
        groupRoots.remove(gid)
        // Tree state is NOT cleared here: if this node sits between remaining members,
        // their beacons still flow through it and handleBeacon keeps the relay entries
        // fresh. Stale entries are pruned naturally by staleEvictionLoop once beacons stop.
        return true
    }

    // -------------------------------------------------------------------------
    // Public API — Multicast
    // -------------------------------------------------------------------------

    /**
     * Sends [payload] to all members of group [gid] via the multicast spanning tree.
     *
     * The frame is pre-marked in [seenMulticasts] to prevent this node from
     * re-delivering its own multicast when relays echo it back via the tree.
     * Silently drops if this node has no registered tree links for [gid] yet
     * (i.e. no beacons have been received, so the tree is not yet built).
     */
    suspend fun sendMulticast(gid: GroupId, payload: ByteArray) {
        if (silent) return
        val seq = multicastSeq.getAndIncrement().toUShort()
        seenMulticasts.markIfNew(selfId, seq)   // pre-mark to suppress our own echo
        val frame = Frame.MulticastFrame(selfId, gid, seq, MAX_TTL, payload)
        multicastTree.allLinksFor(gid).forEach { link ->
            txQueue.enqueue(TxEntry(encode(frame), setOf(link), PacketFlavor.MULTICAST))
        }
    }

    // -------------------------------------------------------------------------
    // Public API — Radio silence
    // -------------------------------------------------------------------------

    /**
     * Completely shuts down the node's radio presence. While silent:
     *  - No OGMs or beacons are emitted, so the node disappears from neighbours'
     *    routing tables after their purge timeout.
     *  - Received frames are still decoded and the local routing table is still
     *    updated, but no frame is ever relayed or forwarded — the node does not
     *    participate in the mesh as a transit hop.
     *  - [send] and [sendMulticast] calls are silently dropped.
     *
     * Call [unsilence] to resume. The node re-announces itself within one OGM cycle.
     */
    fun silence() { silent = true }

    /**
     * Resumes full participation after [silence]. OGM and beacon loops are already
     * running at their scheduled interval, so the node re-announces itself and
     * resumes relaying within one OGM cycle.
     */
    fun unsilence() { silent = false }

    // -------------------------------------------------------------------------
    // Internal loops
    // -------------------------------------------------------------------------

    /**
     * Emits this node's OGM on [link] every [Link.ogmInterval].
     * Each emission uses the next value from [ogmSeq], which is shared across all
     * links so that sequence numbers are globally unique per originator regardless
     * of which link the OGM went out on.
     */
    private suspend fun ogmLoop(link: Link) {
        val tier = link.bandwidthTier
        while (true) {
            if (!silent) {
                val seq = ogmSeq.getAndIncrement().toUShort()
                txQueue.enqueue(TxEntry(
                    frame         = encode(Frame.OgmFrame(Ogm(selfId, selfId, seq, MAX_TTL, tier))),
                    eligibleLinks = setOf(link),
                    flavor        = PacketFlavor.OGM
                ))
            }
            delay(link.ogmInterval)
        }
    }

    /**
     * Pulls frames from [txQueue] for [link] and hands them to [Link.broadcast].
     * Suspends while the queue has no frames for this link, so the coroutine
     * consumes no CPU when the link is idle. The link is effectively "busy" for
     * the duration of each [Link.broadcast] call, which provides natural backpressure.
     */
    private suspend fun txLoop(link: Link) {
        while (true) {
            val entry = txQueue.pollFor(link)
            link.broadcast(entry.frame)
        }
    }

    /**
     * Sends a [Frame.BeaconFrame] toward the current active root every [beaconInterval].
     * Relay nodes that forward this beacon record both legs of the path in
     * [MulticastTree], building this node's branch of the spanning tree.
     *
     * The active root is resolved by [resolveActiveRoot]: the first root with a
     * known route becomes the [Frame.BeaconFrame.activeRoot]. If no candidate is
     * reachable the loop waits one [beaconInterval] and retries.
     * The loop exits cleanly when [gid] is removed from [localGroups].
     *
     * ## Leaf suppression
     *
     * A node that is already relaying beacons from downstream members does not need
     * to originate its own beacon: the relayed beacons already keep the upstream
     * path alive and register this node's links in the tree. The node suppresses
     * its own beacon for any tick where [MulticastTree.hasActiveDownstream] returns
     * true, making it a transparent relay rather than an independent beacon source.
     * If all downstream members go silent and their branches are evicted, this node
     * becomes a leaf again and resumes beaconing on the next tick.
     */
    private suspend fun beaconLoop(gid: GroupId, beaconInterval: Duration) {
        while (gid in localGroups) {
            if (multicastTree.hasActiveDownstream(gid, beaconInterval * 2)) {
                // A downstream member is keeping the path alive — suppress our own beacon.
                delay(beaconInterval)
                continue
            }
            val activeRoot = resolveActiveRoot(gid) ?: run { delay(beaconInterval); return@run null } ?: continue
            val route = neighborTable[activeRoot]   ?: run { delay(beaconInterval); return@run null } ?: continue
            multicastTree.registerUpstream(gid, route.link)
            if (!silent) txQueue.enqueue(TxEntry(
                frame         = encode(Frame.BeaconFrame(route.nextHop, selfId, gid, activeRoot)),
                eligibleLinks = setOf(route.link),
                flavor        = PacketFlavor.BEACON
            ))
            delay(beaconInterval)
        }
    }

    /**
     * Returns the first reachable root candidate for [gid].
     * Returns `null` if no candidates are registered or none currently has a route
     * in [neighborTable].
     */
    private fun resolveActiveRoot(gid: GroupId): NodeId? {
        val candidates = groupRoots[gid] ?: return null
        return candidates.firstOrNull { neighborTable[it] != null }
    }

    /**
     * Periodically removes unreachable entries from [neighborTable].
     *
     * Each entry carries the [Link] it was learned on. An entry is considered stale
     * when no OGM from that originator has been seen for more than
     * [neighborPurgeMultiplier] × [Link.ogmInterval] — i.e. the node missed that
     * many consecutive heartbeat cycles on its best-path link.
     *
     * The loop runs at the interval of the fastest link so that entries for nodes
     * reachable via quick links are evicted promptly. [ConcurrentHashMap.entries.removeIf]
     * is thread-safe and does not require external locking.
     */
    private suspend fun neighborPurgeLoop() {
        while (true) {
            delay(3.seconds)
            val now = Instant.now()
            val removed = neighborTable.entries.removeIf { (_, entry) ->
                val expiryMs = entry.link.ogmInterval.inWholeMilliseconds * neighborPurgeMultiplier
                entry.lastSeen.plusMillis(expiryMs).isBefore(now)
            }
            if (removed) _routes.value = neighborTable.toMap()
        }
    }

    /**
     * Periodically removes stale entries from [multicastTree].
     * Runs every [staleThreshold]/3 to ensure entries expire within one threshold
     * period of the last refresh. A member that disconnects or leaves the group
     * stops sending beacons; after [staleThreshold] time its tree branch is pruned.
     */
    private suspend fun staleEvictionLoop() {
        while (true) {
            delay(staleThreshold / 3)
            multicastTree.evictStale(staleThreshold)
        }
    }

    // -------------------------------------------------------------------------
    // Frame dispatch
    // -------------------------------------------------------------------------

    /**
     * Collects raw bytes from [link], decodes each into a [Frame], and dispatches
     * to the appropriate handler. Unknown or malformed frames are silently dropped.
     */
    private suspend fun receiveLoop(link: Link) {
        link.frames.collect { raw ->
            when (val frame = decode(raw)) {
                is Frame.OgmFrame       -> handleOgm(frame.ogm, link)
                is Frame.DataFrame      -> handleData(frame)
                is Frame.BeaconFrame    -> handleBeacon(frame, link)
                is Frame.MulticastFrame -> handleMulticast(frame, link)
                null                    -> Unit  // malformed or unknown type
            }
        }
    }

    // -------------------------------------------------------------------------
    // Frame handlers
    // -------------------------------------------------------------------------

    /**
     * Processes a received OGM with jitter-based relay suppression.
     *
     * Flow:
     *  1. Drop our own OGMs (echoed back by other nodes).
     *  2. Atomically increment the hearing count for this (originator, seq) pair.
     *     Only the first coroutine to hear the OGM (count == 1) proceeds; the
     *     rest return early after bumping the count, which influences the relay
     *     probability when the jitter timer fires.
     *  3. Gate on [seenOgms] to handle the rare race where two coroutines both
     *     see count == 1 (shouldn't happen with computeIfAbsent but defensive).
     *  4. Update the neighbour table if this is the best route seen for this originator.
     *  5. Schedule a relay after a random jitter window. When the timer fires,
     *     read the final hearing count and relay with probability 1/count.
     *     This ensures that in a dense subnet (where all nodes hear the OGM and
     *     relay it), only ~1/count of them actually transmit, saving bandwidth.
     *  6. Relay is suppressed on the incoming [link] to avoid echoing back to the
     *     sender (waste) and to nodes that already heard the original.
     */
    private suspend fun handleOgm(ogm: Ogm, link: Link) {
        if (ogm.originatorId == selfId) return

        val key = ogm.originatorId to ogm.seqNum
        // computeIfAbsent is atomic in ConcurrentHashMap: only one coroutine creates the counter.
        val counter = pendingRelays.computeIfAbsent(key) { AtomicInteger(0) }
        val hearingCount = counter.incrementAndGet()

        // Every node on a shared medium must update its routing table when it hears an OGM.
        // Relay suppression (below) reduces redundant retransmissions but must not block this,
        // or nodes that lose the hearingCount==1 race would never refresh their entries.
        updateNeighborTable(ogm, link)

        // Only the first hearer schedules the relay; subsequent hearers just bump the count.
        if (hearingCount > 1) return

        // Defensive gate: seenOgms catches the rare case where the entry was already processed.
        if (!seenOgms.markIfNew(ogm.originatorId, ogm.seqNum)) {
            pendingRelays.remove(key)
            return
        }

        if (ogm.ttl <= 1u) {
            // TTL exhausted — do not relay, but do update the routing table.
            pendingRelays.remove(key)
            return
        }

        // Common relay fields: sender and TTL are the same for all outgoing links.
        // minBandwidthTier varies per outgoing link — each link stamps its own tier
        // as the new bottleneck minimum.
        val relayBase = ogm.copy(senderId = selfId, ttl = (ogm.ttl - 1u).toUByte())

        // Jitter window: a random fraction of the link's OGM interval.
        // Dividing by a random value in [8, 11] gives roughly 9–12.5% of the interval,
        // which is long enough for neighbouring nodes' relays to arrive and increment
        // the hearing count, but short relative to the OGM interval itself.
        val jitterMs = link.ogmInterval.inWholeMilliseconds / (8L + Random.nextLong(0L, 4L))

        scope?.launch {
            delay(jitterMs)
            // Read and remove the counter atomically; default to 1 if already removed.
            val finalCount = pendingRelays.remove(key)?.get() ?: 1
            if (!silent && shouldRelay(finalCount)) {
                // Relay on all links except the one the OGM arrived on.
                // Skipping the incoming link avoids pointless retransmission back
                // toward the sender and halves bandwidth use in sparse chains.
                linksMap.values.filter { it.id != link.id }.forEach { outLink ->
                    val relay = relayBase.copy(
                        minBandwidthTier = minOf(ogm.minBandwidthTier, outLink.bandwidthTier)
                    )
                    txQueue.enqueue(TxEntry(encode(Frame.OgmFrame(relay)), setOf(outLink), PacketFlavor.OGM))
                }
            }
        }
    }

    /**
     * Decides whether to relay an OGM based on how many copies were heard during
     * the jitter window. Probability = 1/[hearingCount]:
     *   count=1 → 100%  (only copy; must relay)
     *   count=2 → 50%   (one relay already sent; maybe a second for resilience)
     *   count=3 → 33%   (two relays sent; low chance of a third)
     *   count=N → 1/N   (decays naturally; expected total relays ≈ ln(N))
     */
    private fun shouldRelay(hearingCount: Int): Boolean =
        Random.nextFloat() < 1.0f / hearingCount

    /**
     * Updates [neighborTable] when an OGM from [ogm.originatorId] is received.
     *
     * Three cases:
     *
     *  1. **Better or equal path**: primary metric is [Ogm.minBandwidthTier] (wider
     *     bottleneck wins); TTL is a tiebreaker when tiers are equal (fewer hops wins).
     *     Replace the entry. Equal-quality OGMs keep re-electing the same next hop each
     *     cycle, so [lastSeen] is refreshed and the entry never goes stale.
     *
     *  2. **Worse path, same next hop and link**: the originator is still reachable via
     *     the recorded next hop at temporarily degraded quality. Refresh [lastSeen] only
     *     to keep the entry alive; preserve the better recorded metric.
     *
     *     This applies when OGMs from the same next hop arrive with varying tiers across
     *     OGM cycles (e.g. relay suppression happened to pick a slower intermediate hop
     *     in this round). The stored tier can therefore only increase, never decrease,
     *     for a given next-hop entry — it represents the best ever observed, not the
     *     instantaneous value. If the upstream relay path permanently degrades without
     *     the next hop disappearing, the stored tier will be stale-optimistic until the
     *     entry is evicted and reinstalled. This mirrors BATMAN's "keep the best" design
     *     and trades accuracy for noise resistance.
     *
     *  3. **Worse path, different next hop or link**: a lower-quality alternative exists
     *     but gives no evidence the current next hop is still forwarding. Leave unchanged.
     *     After [neighborPurgeMultiplier] × [Link.ogmInterval] of silence the entry
     *     expires and the alternative installs itself.
     */
    private fun updateNeighborTable(ogm: Ogm, link: Link) {
        neighborTable.compute(ogm.originatorId) { _, current ->
            val now = Instant.now()
            when {
                current == null
                || ogm.minBandwidthTier > current.minBandwidthTier
                || (ogm.minBandwidthTier == current.minBandwidthTier && ogm.ttl >= current.bestTtl) ->
                    NeighborEntry(
                        nextHop          = ogm.senderId,
                        link             = link,
                        minBandwidthTier = ogm.minBandwidthTier,
                        bestTtl          = ogm.ttl,
                        lastSeq          = ogm.seqNum,
                        lastSeen         = now
                    )
                ogm.senderId == current.nextHop && link.id == current.link.id ->
                    current.copy(lastSeq = ogm.seqNum, lastSeen = now)
                else -> current
            }
        }
        _routes.value = neighborTable.toMap()
    }

    /**
     * Handles a unicast [Frame.DataFrame]:
     *  - Drops frames not addressed to this node ([nextHop] check).
     *  - Delivers to [incomingData] if this is the final destination.
     *  - Forwards to the next hop otherwise, decrementing TTL.
     *    Drops silently if no route is known or TTL has reached zero.
     */
    private suspend fun handleData(frame: Frame.DataFrame) {
        if (frame.nextHop != selfId) return
        if (frame.dstId == selfId) {
            _incomingData.emit(frame.srcId to frame.payload)
            return
        }
        if (silent) return
        if (frame.ttl == 0u.toUByte()) return
        val route = neighborTable[frame.dstId] ?: return
        txQueue.enqueue(TxEntry(
            frame         = encode(frame.copy(nextHop = route.nextHop, ttl = (frame.ttl - 1u).toUByte())),
            eligibleLinks = setOf(route.link),
            flavor        = PacketFlavor.DATA
        ))
    }

    /**
     * Handles a [Frame.BeaconFrame] travelling from a group member toward the active root.
     *
     *  - Drops frames not addressed to this node ([nextHop] check).
     *  - Registers the incoming link in [multicastTree] — this node is on the path
     *    between the beacon's source and the active root.
     *  - If this node IS the active root ([frame.activeRoot] == [selfId]), the tree
     *    entry is sufficient; no further relay needed.
     *  - Otherwise, registers the outgoing link and forwards the beacon one hop closer
     *    to [frame.activeRoot], so the next relay can also record its two links.
     *
     * Using [frame.activeRoot] (rather than any GroupId-embedded field) allows the
     * spanning tree to be rooted at any designated root node, without any relay node
     * needing to know about the group's root list.
     */
    private suspend fun handleBeacon(frame: Frame.BeaconFrame, incomingLink: Link) {
        if (frame.nextHop != selfId) return

        multicastTree.registerDownstream(frame.groupId, incomingLink)

        if (frame.activeRoot == selfId) return  // we are the current root; tree entry is enough
        if (silent) return

        val route = neighborTable[frame.activeRoot] ?: return
        multicastTree.registerUpstream(frame.groupId, route.link)
        txQueue.enqueue(TxEntry(
            frame         = encode(frame.copy(nextHop = route.nextHop)),
            eligibleLinks = setOf(route.link),
            flavor        = PacketFlavor.BEACON
        ))
    }

    /**
     * Handles an incoming [Frame.MulticastFrame]:
     *  1. Deduplicates via [seenMulticasts] — drops if already seen.
     *  2. Delivers to [incomingMulticast] if this node is a member of the group.
     *  3. Forwards on all other tree links for this group (excluding the incoming
     *     link to prevent echo), decrementing TTL. If [multicastTree] has no
     *     outgoing links for this group the frame is not forwarded (leaf node).
     */
    private suspend fun handleMulticast(frame: Frame.MulticastFrame, incomingLink: Link) {
        if (!seenMulticasts.markIfNew(frame.srcId, frame.seqNum)) return

        if (frame.groupId in localGroups) {
            _incomingMulticast.emit(MulticastMessage(frame.srcId, frame.groupId, frame.payload))
        }

        if (silent) return
        if (frame.ttl == 0u.toUByte()) return

        val relayed = frame.copy(ttl = (frame.ttl - 1u).toUByte())
        multicastTree.linksFor(frame.groupId, except = incomingLink).forEach { outLink ->
            txQueue.enqueue(TxEntry(encode(relayed), setOf(outLink), PacketFlavor.MULTICAST))
        }
    }
}
