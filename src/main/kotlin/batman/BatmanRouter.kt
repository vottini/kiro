package batman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
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
 * Payload delivered to [BatmanRouter.incomingMulticast] collectors when a
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
 * A group owner calls [createGroup] to obtain a [GroupId], then [invite]s members.
 * Each invited member joins via [joinGroup] and begins sending periodic
 * [Frame.BeaconFrame]s toward the owner. Relay nodes record both legs of each
 * beacon's path in [multicastTree], building a spanning tree. Any group member or
 * the owner can then call [sendMulticast] to reach all members via tree-based
 * forwarding rather than network-wide flooding.
 *
 * ## Concurrency model
 *
 * All per-link loops (receive, OGM emit, TX drain) are launched as coroutines
 * within the [CoroutineScope] passed to [start]. State shared across coroutines
 * ([neighborTable], [pendingRelays], [multicastTree]) uses [ConcurrentHashMap]
 * and [AtomicInteger] to avoid explicit locking.
 *
 * @param selfId This node's unique identifier within the mesh.
 * @param links All radio interfaces available to this node.
 * @param txQueue Central transmit queue; injectable for testing.
 * @param staleThreshold How long without a beacon refresh before a multicast tree
 *   branch is considered dead and evicted. Should be at least 3× the beacon interval.
 * @param neighborPurgeMultiplier How many missed OGM cycles before a neighbour table
 *   entry is considered unreachable and removed. A node that has been silent for
 *   [neighborPurgeMultiplier] × [Link.ogmInterval] is presumed gone. Default 3 matches
 *   batman-adv's own originator timeout heuristic.
 */
class BatmanRouter(
    val selfId: NodeId,
    val links: List<Link>,
    val txQueue: TxQueue = TxQueue(),
    val staleThreshold: Duration = 90.seconds,
    val neighborPurgeMultiplier: Int = 3
) {
    // --- Unicast routing state ---

    /** Next-hop routing table: originator NodeId → best known route entry. */
    private val neighborTable = ConcurrentHashMap<NodeId, NeighborEntry>()

    /** Deduplicates OGMs so each (originator, seq) is processed and relayed at most once. */
    private val seenOgms = SeenWindowCache()

    /** Global OGM sequence counter shared across all links on this node. */
    private val ogmSeq = AtomicInteger(0)

    /**
     * Tracks how many times each pending OGM relay has been heard during its jitter window.
     * Key: (originatorId, seqNum). Value: hearing count (starts at 1 on first receipt).
     * Entries are created when the first copy of an OGM arrives and removed when
     * the jitter timer fires and the relay decision is made.
     */
    private val pendingRelays = ConcurrentHashMap<Pair<NodeId, UShort>, AtomicInteger>()

    // --- Multicast state ---

    /** Spanning tree of links per group, built from beacon relay observations. */
    private val multicastTree = MulticastTree()

    /** Deduplicates multicast frames so each (srcId, seqNum) is delivered/relayed once. */
    private val seenMulticasts = SeenWindowCache()

    /** Per-node multicast sequence counter for frames originated by this node. */
    private val multicastSeq = AtomicInteger(0)

    // --- Group membership ---

    /** Set of [GroupId]s this node belongs to, either as owner or as member. */
    private val localGroups = ConcurrentHashMap.newKeySet<GroupId>()

    /**
     * Groups owned by this node, mapping groupId to the set of invited members.
     * Only populated on the owner node; used to track who has been invited.
     */
    private val ownedGroups = ConcurrentHashMap<GroupId, MutableSet<NodeId>>()

    /**
     * Ordered fallback roots per group, stored when this node joins via an invite.
     * [beaconLoop] tries [GroupId.owner] first, then each deputy in order, sending
     * beacons toward the first one that has a known route in [neighborTable].
     */
    private val groupDeputies = ConcurrentHashMap<GroupId, List<NodeId>>()

    /** Counter for generating unique sequential group IDs on this node (owner role). */
    private val groupSeq = AtomicInteger(0)

    // --- Public flows ---

    private val _incomingData = MutableSharedFlow<Pair<NodeId, ByteArray>>()
    /** Emits (srcId, payload) whenever a unicast [Frame.DataFrame] addressed to this node arrives. */
    val incomingData: SharedFlow<Pair<NodeId, ByteArray>> = _incomingData

    private val _incomingMulticast = MutableSharedFlow<MulticastMessage>()
    /** Emits a [MulticastMessage] whenever a group multicast arrives and this node is a member. */
    val incomingMulticast: SharedFlow<MulticastMessage> = _incomingMulticast

    /** Stored on [start] so that [joinGroup] can launch beacon coroutines after startup. */
    private lateinit var scope: CoroutineScope

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts all per-link coroutines and the stale-entry eviction loop.
     * Must be called exactly once before any other method.
     */
    fun start(scope: CoroutineScope) {
        this.scope = scope
        links.forEach { link ->
            scope.launch { receiveLoop(link) }   // inbound frame dispatch
            scope.launch { ogmLoop(link) }        // periodic OGM heartbeat
            scope.launch { txLoop(link) }         // pulls from TxQueue and calls link.broadcast
        }
        scope.launch { staleEvictionLoop() }      // prunes dead multicast tree branches
        scope.launch { neighborPurgeLoop() }      // evicts unreachable neighbour table entries
    }

    // -------------------------------------------------------------------------
    // Public API — Unicast
    // -------------------------------------------------------------------------

    /**
     * Sends [payload] to [dstId] via the best known next-hop route.
     * Silently drops the frame if no route to [dstId] exists yet.
     */
    suspend fun send(dstId: NodeId, payload: ByteArray) {
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
    // Public API — Group management (owner)
    // -------------------------------------------------------------------------

    /**
     * Creates a new multicast group owned by this node.
     * The returned [GroupId] encodes this node's [selfId] in the high 16 bits and
     * a monotonically increasing sequence number in the low 16 bits, ensuring
     * global uniqueness without coordination.
     */
    fun createGroup(): GroupId {
        val gid = GroupId(selfId, groupSeq.getAndIncrement().toUShort())
        localGroups.add(gid)
        ownedGroups[gid] = ConcurrentHashMap.newKeySet<NodeId>().also { it.add(selfId) }
        return gid
    }

    /**
     * Sends a [Frame.InviteFrame] to [memberId] for group [gid].
     * The invited node will call [joinGroup] upon receipt and begin sending beacons,
     * which builds its branch of the multicast spanning tree.
     *
     * [deputies] is an ordered list of fallback tree roots. If the primary owner
     * becomes unreachable, the invitee will beacon toward the first deputy that has
     * a known route, keeping the spanning tree alive without intervention.
     *
     * Silently drops if no route to [memberId] is known yet.
     */
    suspend fun invite(gid: GroupId, memberId: NodeId, deputies: List<NodeId> = emptyList()) {
        val route = neighborTable[memberId] ?: return
        ownedGroups[gid]?.add(memberId)
        txQueue.enqueue(TxEntry(
            frame         = encode(Frame.InviteFrame(route.nextHop, selfId, memberId, gid, deputies)),
            eligibleLinks = setOf(route.link),
            flavor        = PacketFlavor.INVITE
        ))
    }

    // -------------------------------------------------------------------------
    // Public API — Group membership (member)
    // -------------------------------------------------------------------------

    /**
     * Registers this node as a member of [gid] and starts the beacon loop that
     * maintains this node's branch in the multicast spanning tree.
     *
     * [deputies] is stored in [groupDeputies] and consulted by [beaconLoop] whenever
     * the primary owner is unreachable: the first deputy with a known route becomes
     * the temporary [Frame.BeaconFrame.activeRoot], keeping the tree alive.
     *
     * [beaconInterval] controls how often beacons are sent. It should be shorter
     * than [staleThreshold] (default: staleThreshold/3) so the tree branch does
     * not expire between refreshes. Slower links may warrant longer intervals.
     */
    fun joinGroup(
        gid: GroupId,
        deputies: List<NodeId> = emptyList(),
        beaconInterval: Duration = staleThreshold / 3
    ) {
        localGroups.add(gid)
        if (deputies.isNotEmpty()) groupDeputies[gid] = deputies
        scope.launch { beaconLoop(gid, beaconInterval) }
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
        val seq = multicastSeq.getAndIncrement().toUShort()
        seenMulticasts.markIfNew(selfId, seq)   // pre-mark to suppress our own echo
        val frame = Frame.MulticastFrame(selfId, gid, seq, MAX_TTL, payload)
        multicastTree.allLinksFor(gid).forEach { link ->
            txQueue.enqueue(TxEntry(encode(frame), setOf(link), PacketFlavor.MULTICAST))
        }
    }

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
        while (true) {
            val seq = ogmSeq.getAndIncrement().toUShort()
            txQueue.enqueue(TxEntry(encode(Frame.OgmFrame(Ogm(selfId, selfId, seq, MAX_TTL))), setOf(link), PacketFlavor.OGM))
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
     * The active root is resolved by [resolveActiveRoot]: the primary owner is tried
     * first; if unreachable, deputies from [groupDeputies] are tried in order. If no
     * candidate is reachable the loop waits one [beaconInterval] and retries.
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
            txQueue.enqueue(TxEntry(
                frame         = encode(Frame.BeaconFrame(route.nextHop, selfId, gid, activeRoot)),
                eligibleLinks = setOf(route.link),
                flavor        = PacketFlavor.BEACON
            ))
            delay(beaconInterval)
        }
    }

    /**
     * Returns the first reachable root candidate for [gid]: the primary owner first,
     * then each deputy in the order they were listed in the invite. Returns `null`
     * if none of the candidates currently have a route in [neighborTable].
     */
    private fun resolveActiveRoot(gid: GroupId): NodeId? {
        val candidates = listOf(gid.owner) + (groupDeputies[gid] ?: emptyList())
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
        val checkInterval = links.minOf { it.ogmInterval }
        while (true) {
            delay(checkInterval)
            val now = Instant.now()
            neighborTable.entries.removeIf { (_, entry) ->
                val expiryMs = entry.link.ogmInterval.inWholeMilliseconds * neighborPurgeMultiplier
                entry.lastSeen.plusMillis(expiryMs).isBefore(now)
            }
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
                is Frame.InviteFrame    -> handleInvite(frame)
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

        // Only the first hearer schedules the relay; subsequent hearers just bump the count.
        if (hearingCount > 1) return

        // Defensive gate: seenOgms catches the rare case where the entry was already processed.
        if (!seenOgms.markIfNew(ogm.originatorId, ogm.seqNum)) {
            pendingRelays.remove(key)
            return
        }

        updateNeighborTable(ogm, link)

        if (ogm.ttl <= 1u) {
            // TTL exhausted — do not relay, but do update the routing table.
            pendingRelays.remove(key)
            return
        }

        val relay = ogm.copy(senderId = selfId, ttl = (ogm.ttl - 1u).toUByte())

        // Jitter window: a random fraction of the link's OGM interval.
        // Dividing by a random value in [8, 11] gives roughly 9–12.5% of the interval,
        // which is long enough for neighbouring nodes' relays to arrive and increment
        // the hearing count, but short relative to the OGM interval itself.
        val jitterMs = link.ogmInterval.inWholeMilliseconds / (8L + Random.nextLong(0L, 4L))

        scope.launch {
            delay(jitterMs)
            // Read and remove the counter atomically; default to 1 if already removed.
            val finalCount = pendingRelays.remove(key)?.get() ?: 1
            if (shouldRelay(finalCount)) {
                // Relay on all links except the one the OGM arrived on.
                // Skipping the incoming link avoids pointless retransmission back
                // toward the sender and halves bandwidth use in sparse chains.
                links.filter { it.id != link.id }.forEach { outLink ->
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
     * Updates [neighborTable] if [ogm] represents a better (higher TTL) path to
     * its originator than what is currently recorded. Higher TTL means fewer hops
     * were traversed, so this node prefers the neighbour ([ogm.senderId]) that
     * delivered the OGM with the most remaining TTL.
     */
    private fun updateNeighborTable(ogm: Ogm, link: Link) {
        val current = neighborTable[ogm.originatorId]
        if (current == null || ogm.ttl > current.bestTtl) {
            neighborTable[ogm.originatorId] = NeighborEntry(
                nextHop  = ogm.senderId,
                link     = link,
                bestTtl  = ogm.ttl,
                lastSeq  = ogm.seqNum,
                lastSeen = Instant.now()
            )
        }
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
     * Using [frame.activeRoot] (rather than the GroupId's embedded owner) allows the
     * spanning tree to be rooted at a deputy when the primary owner is offline, without
     * any relay node needing to know about the deputy list.
     */
    private suspend fun handleBeacon(frame: Frame.BeaconFrame, incomingLink: Link) {
        if (frame.nextHop != selfId) return

        multicastTree.registerDownstream(frame.groupId, incomingLink)

        if (frame.activeRoot == selfId) return  // we are the current root; tree entry is enough

        val route = neighborTable[frame.activeRoot] ?: return
        multicastTree.registerUpstream(frame.groupId, route.link)
        txQueue.enqueue(TxEntry(
            frame         = encode(frame.copy(nextHop = route.nextHop)),
            eligibleLinks = setOf(route.link),
            flavor        = PacketFlavor.BEACON
        ))
    }

    /**
     * Handles a [Frame.InviteFrame] from a group owner:
     *  - Drops frames not addressed to this node ([nextHop] check).
     *  - If this is the final destination, calls [joinGroup] to register membership
     *    and start the beacon loop that builds this node's tree branch.
     *  - Otherwise, forwards the invite toward [dstId] via unicast routing.
     */
    private suspend fun handleInvite(frame: Frame.InviteFrame) {
        if (frame.nextHop != selfId) return
        if (frame.dstId == selfId) {
            joinGroup(frame.groupId, frame.deputies)
            return
        }
        val route = neighborTable[frame.dstId] ?: return
        txQueue.enqueue(TxEntry(
            frame         = encode(frame.copy(nextHop = route.nextHop)),
            eligibleLinks = setOf(route.link),
            flavor        = PacketFlavor.INVITE
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

        if (frame.ttl == 0u.toUByte()) return

        val relayed = frame.copy(ttl = (frame.ttl - 1u).toUByte())
        multicastTree.linksFor(frame.groupId, except = incomingLink).forEach { outLink ->
            txQueue.enqueue(TxEntry(encode(relayed), setOf(outLink), PacketFlavor.MULTICAST))
        }
    }
}
