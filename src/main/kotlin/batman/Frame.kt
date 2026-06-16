package batman

data class Ogm(
    val originatorId: NodeId,
    val senderId: NodeId,
    val seqNum: UShort,
    val ttl: UByte
)

sealed class Frame {
    data class OgmFrame(val ogm: Ogm) : Frame()
    data class DataFrame(
        val nextHop: NodeId,
        val srcId: NodeId,
        val dstId: NodeId,
        val ttl: UByte,
        val payload: ByteArray
    ) : Frame() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is DataFrame) return false
            return nextHop == other.nextHop &&
                srcId == other.srcId &&
                dstId == other.dstId &&
                ttl == other.ttl &&
                payload.contentEquals(other.payload)
        }

        override fun hashCode(): Int {
            var result = nextHop.hashCode()
            result = 31 * result + srcId.hashCode()
            result = 31 * result + dstId.hashCode()
            result = 31 * result + ttl.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }
}
