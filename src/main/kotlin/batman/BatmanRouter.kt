package batman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

const val MAX_TTL: UByte = 50u

data class MulticastMessage(val srcId: NodeId, val groupId: GroupId, val payload: ByteArray)

class BatmanRouter(
    val selfId: NodeId,
    val links: List<Link>,
    val txQueue: TxQueue = TxQueue(),
    val staleThreshold: Duration = 90.seconds
) {
    private val neighborTable = ConcurrentHashMap<NodeId, NeighborEntry>()
    private val seenOgms = SeenWindowCache()
    private val ogmSeq = AtomicInteger(0)

    private val multicastTree = MulticastTree()
    private val seenMulticasts = SeenWindowCache()
    private val multicastSeq = AtomicInteger(0)

    // Groups this node belongs to (as owner or member)
    private val localGroups = ConcurrentHashMap.newKeySet<GroupId>()

    // Groups this node owns: groupId -> member set (for sending invites)
    private val ownedGroups = ConcurrentHashMap<GroupId, MutableSet<NodeId>>()
    private val groupSeq = AtomicInteger(0)

    private val _incomingData = MutableSharedFlow<Pair<NodeId, ByteArray>>()
    val incomingData: SharedFlow<Pair<NodeId, ByteArray>> = _incomingData

    private val _incomingMulticast = MutableSharedFlow<MulticastMessage>()
    val incomingMulticast: SharedFlow<MulticastMessage> = _incomingMulticast

    private lateinit var scope: CoroutineScope

    fun start(scope: CoroutineScope) {
        this.scope = scope
        links.forEach { link ->
            scope.launch { receiveLoop(link) }
            scope.launch { ogmLoop(link) }
            scope.launch { txLoop(link) }
        }
        scope.launch { staleEvictionLoop() }
    }

    // --- Unicast ---

    suspend fun send(dstId: NodeId, payload: ByteArray) {
        val route = neighborTable[dstId] ?: return
        val frame = Frame.DataFrame(
            nextHop = route.nextHop,
            srcId   = selfId,
            dstId   = dstId,
            ttl     = MAX_TTL,
            payload = payload
        )
        txQueue.enqueue(TxEntry(encode(frame), route.link, TxPriority.DATA))
    }

    // --- Group management (owner) ---

    fun createGroup(): GroupId {
        val gid = groupId(selfId, groupSeq.getAndIncrement().toUShort())
        localGroups.add(gid)
        ownedGroups[gid] = ConcurrentHashMap.newKeySet<NodeId>().also { it.add(selfId) }
        return gid
    }

    suspend fun invite(gid: GroupId, memberId: NodeId) {
        val route = neighborTable[memberId] ?: return
        ownedGroups[gid]?.add(memberId)
        txQueue.enqueue(TxEntry(
            frame      = encode(Frame.InviteFrame(route.nextHop, selfId, memberId, gid)),
            targetLink = route.link,
            priority   = TxPriority.CONTROL
        ))
    }

    // --- Group membership (member) ---

    fun joinGroup(gid: GroupId, beaconInterval: Duration = staleThreshold / 3) {
        localGroups.add(gid)
        scope.launch { beaconLoop(gid, beaconInterval) }
    }

    // --- Multicast send ---

    suspend fun sendMulticast(gid: GroupId, payload: ByteArray) {
        val seq = multicastSeq.getAndIncrement().toUShort()
        seenMulticasts.markIfNew(selfId, seq)   // suppress our own echo
        val frame = Frame.MulticastFrame(selfId, gid, seq, MAX_TTL, payload)
        multicastTree.allLinksFor(gid).forEach { link ->
            txQueue.enqueue(TxEntry(encode(frame), link, TxPriority.DATA))
        }
    }

    // --- Internal loops ---

    private suspend fun ogmLoop(link: Link) {
        while (true) {
            val seq = ogmSeq.getAndIncrement().toUShort()
            txQueue.enqueue(TxEntry(encode(Frame.OgmFrame(Ogm(selfId, selfId, seq, MAX_TTL))), link, TxPriority.CONTROL))
            delay(link.ogmInterval)
        }
    }

    private suspend fun txLoop(link: Link) {
        while (true) {
            val entry = txQueue.pollFor(link)
            link.broadcast(entry.frame)
        }
    }

    private suspend fun beaconLoop(gid: GroupId, beaconInterval: Duration) {
        while (gid in localGroups) {
            val route = neighborTable[gid.owner()] ?: run { delay(beaconInterval); return@run null } ?: continue
            multicastTree.registerLink(gid, route.link)
            txQueue.enqueue(TxEntry(
                frame      = encode(Frame.BeaconFrame(route.nextHop, selfId, gid)),
                targetLink = route.link,
                priority   = TxPriority.CONTROL
            ))
            delay(beaconInterval)
        }
    }

    private suspend fun staleEvictionLoop() {
        while (true) {
            delay(staleThreshold / 3)
            multicastTree.evictStale(staleThreshold)
        }
    }

    // --- Frame dispatch ---

    private suspend fun receiveLoop(link: Link) {
        link.frames.collect { raw ->
            when (val frame = decode(raw)) {
                is Frame.OgmFrame       -> handleOgm(frame.ogm, link)
                is Frame.DataFrame      -> handleData(frame)
                is Frame.BeaconFrame    -> handleBeacon(frame, link)
                is Frame.InviteFrame    -> handleInvite(frame)
                is Frame.MulticastFrame -> handleMulticast(frame, link)
                null                    -> Unit
            }
        }
    }

    // --- Frame handlers ---

    private suspend fun handleOgm(ogm: Ogm, link: Link) {
        if (ogm.originatorId == selfId) return
        if (!seenOgms.markIfNew(ogm.originatorId, ogm.seqNum)) return

        updateNeighborTable(ogm, link)

        if (ogm.ttl > 1u) {
            val relay = ogm.copy(senderId = selfId, ttl = (ogm.ttl - 1u).toUByte())
            links.forEach { outLink ->
                txQueue.enqueue(TxEntry(encode(Frame.OgmFrame(relay)), outLink, TxPriority.CONTROL))
            }
        }
    }

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

    private suspend fun handleData(frame: Frame.DataFrame) {
        if (frame.nextHop != selfId) return
        if (frame.dstId == selfId) {
            _incomingData.emit(frame.srcId to frame.payload)
            return
        }
        if (frame.ttl == 0u.toUByte()) return
        val route = neighborTable[frame.dstId] ?: return
        txQueue.enqueue(TxEntry(
            frame      = encode(frame.copy(nextHop = route.nextHop, ttl = (frame.ttl - 1u).toUByte())),
            targetLink = route.link,
            priority   = TxPriority.DATA
        ))
    }

    private suspend fun handleBeacon(frame: Frame.BeaconFrame, incomingLink: Link) {
        if (frame.nextHop != selfId) return

        multicastTree.registerLink(frame.groupId, incomingLink)

        // We are the owner: tree entry recorded, nothing more to do
        if (frame.groupId.owner() == selfId) return

        // Relay beacon one hop closer to the owner
        val route = neighborTable[frame.groupId.owner()] ?: return
        multicastTree.registerLink(frame.groupId, route.link)
        txQueue.enqueue(TxEntry(
            frame      = encode(frame.copy(nextHop = route.nextHop)),
            targetLink = route.link,
            priority   = TxPriority.CONTROL
        ))
    }

    private suspend fun handleInvite(frame: Frame.InviteFrame) {
        if (frame.nextHop != selfId) return
        if (frame.dstId == selfId) {
            joinGroup(frame.groupId)
            return
        }
        val route = neighborTable[frame.dstId] ?: return
        txQueue.enqueue(TxEntry(
            frame      = encode(frame.copy(nextHop = route.nextHop)),
            targetLink = route.link,
            priority   = TxPriority.CONTROL
        ))
    }

    private suspend fun handleMulticast(frame: Frame.MulticastFrame, incomingLink: Link) {
        if (!seenMulticasts.markIfNew(frame.srcId, frame.seqNum)) return

        if (frame.groupId in localGroups) {
            _incomingMulticast.emit(MulticastMessage(frame.srcId, frame.groupId, frame.payload))
        }

        if (frame.ttl == 0u.toUByte()) return

        val relayed = frame.copy(ttl = (frame.ttl - 1u).toUByte())
        multicastTree.linksFor(frame.groupId, except = incomingLink).forEach { outLink ->
            txQueue.enqueue(TxEntry(encode(relayed), outLink, TxPriority.DATA))
        }
    }
}
