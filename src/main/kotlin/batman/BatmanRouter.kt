package batman

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

const val MAX_TTL: UByte = 50u

class BatmanRouter(
    val selfId: NodeId,
    val links: List<Link>,
    val txQueue: TxQueue = TxQueue()
) {
    private val neighborTable = ConcurrentHashMap<NodeId, NeighborEntry>()
    private val seenOgms = SeenWindowCache()
    private val ogmSeq = AtomicInteger(0)

    private val _incomingData = MutableSharedFlow<Pair<NodeId, ByteArray>>()
    val incomingData: SharedFlow<Pair<NodeId, ByteArray>> = _incomingData

    fun start(scope: CoroutineScope) {
        links.forEach { link ->
            scope.launch { receiveLoop(link) }
            scope.launch { ogmLoop(link) }
            scope.launch { txLoop(link) }
        }
    }

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

    private suspend fun ogmLoop(link: Link) {
        while (true) {
            val seq = ogmSeq.getAndIncrement().toUShort()
            val ogm = Ogm(selfId, selfId, seq, MAX_TTL)
            txQueue.enqueue(TxEntry(encode(Frame.OgmFrame(ogm)), link, TxPriority.CONTROL))
            delay(link.ogmInterval)
        }
    }

    private suspend fun txLoop(link: Link) {
        while (true) {
            val entry = txQueue.pollFor(link)
            link.broadcast(entry.frame)
        }
    }

    private suspend fun receiveLoop(link: Link) {
        link.frames.collect { raw ->
            when (val frame = decode(raw)) {
                is Frame.OgmFrame  -> handleOgm(frame.ogm, link)
                is Frame.DataFrame -> handleData(frame)
                null               -> Unit
            }
        }
    }

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
}
