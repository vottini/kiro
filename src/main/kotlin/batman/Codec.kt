package batman

import java.nio.ByteBuffer

private const val FRAME_OGM: Byte = 0x01
private const val FRAME_DATA: Byte = 0x02

fun encode(frame: Frame): ByteArray = when (frame) {
    is Frame.OgmFrame -> {
        val ogm = frame.ogm
        ByteBuffer.allocate(8)
            .put(FRAME_OGM)
            .putShort(ogm.originatorId.toShort())
            .putShort(ogm.senderId.toShort())
            .putShort(ogm.seqNum.toShort())
            .put(ogm.ttl.toByte())
            .array()
    }
    is Frame.DataFrame -> {
        ByteBuffer.allocate(10 + frame.payload.size)
            .put(FRAME_DATA)
            .putShort(frame.nextHop.toShort())
            .putShort(frame.srcId.toShort())
            .putShort(frame.dstId.toShort())
            .put(frame.ttl.toByte())
            .putShort(frame.payload.size.toShort())
            .put(frame.payload)
            .array()
    }
}

fun decode(raw: ByteArray): Frame? {
    if (raw.isEmpty()) return null
    val buf = ByteBuffer.wrap(raw)
    return when (buf.get()) {
        FRAME_OGM -> {
            if (raw.size < 8) return null
            Frame.OgmFrame(Ogm(
                originatorId = buf.short.toUShort(),
                senderId     = buf.short.toUShort(),
                seqNum       = buf.short.toUShort(),
                ttl          = buf.get().toUByte()
            ))
        }
        FRAME_DATA -> {
            if (raw.size < 10) return null
            val nextHop    = buf.short.toUShort()
            val srcId      = buf.short.toUShort()
            val dstId      = buf.short.toUShort()
            val ttl        = buf.get().toUByte()
            val payloadLen = buf.short.toInt()
            if (raw.size < 10 + payloadLen) return null
            val payload = ByteArray(payloadLen).also { buf.get(it) }
            Frame.DataFrame(nextHop, srcId, dstId, ttl, payload)
        }
        else -> null
    }
}
