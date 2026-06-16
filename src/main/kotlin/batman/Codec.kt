package batman

import java.nio.ByteBuffer

/**
 * Wire-format type tags. The first byte of every frame identifies its type
 * so [decode] knows how many bytes to read and how to interpret them.
 */
private const val FRAME_OGM: Byte       = 0x01
private const val FRAME_DATA: Byte      = 0x02
private const val FRAME_BEACON: Byte    = 0x03
private const val FRAME_MULTICAST: Byte = 0x04
private const val FRAME_INVITE: Byte    = 0x05

/**
 * Serialises a [Frame] into a compact binary representation suitable for
 * transmission over band-limited radio links. All multi-byte integers are
 * big-endian (Java [ByteBuffer] default).
 *
 * Wire layouts (all sizes in bytes):
 *
 *   OGM       [0x01 | originatorId(2) | senderId(2) | seqNum(2) | ttl(1)]          = 8 B
 *   DATA      [0x02 | nextHop(2) | srcId(2) | dstId(2) | ttl(1) | len(2) | payload] = 10+n B
 *   BEACON    [0x03 | nextHop(2) | srcId(2) | groupId(4)]                           = 9 B
 *   INVITE    [0x05 | nextHop(2) | srcId(2) | dstId(2) | groupId(4)]               = 11 B
 *   MULTICAST [0x04 | srcId(2) | groupId(4) | seqNum(2) | ttl(1) | len(2) | payload] = 12+n B
 */
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
    is Frame.BeaconFrame -> {
        ByteBuffer.allocate(9)
            .put(FRAME_BEACON)
            .putShort(frame.nextHop.toShort())
            .putShort(frame.srcId.toShort())
            .putInt(frame.groupId.toInt())
            .array()
    }
    is Frame.InviteFrame -> {
        ByteBuffer.allocate(11)
            .put(FRAME_INVITE)
            .putShort(frame.nextHop.toShort())
            .putShort(frame.srcId.toShort())
            .putShort(frame.dstId.toShort())
            .putInt(frame.groupId.toInt())
            .array()
    }
    is Frame.MulticastFrame -> {
        ByteBuffer.allocate(12 + frame.payload.size)
            .put(FRAME_MULTICAST)
            .putShort(frame.srcId.toShort())
            .putInt(frame.groupId.toInt())
            .putShort(frame.seqNum.toShort())
            .put(frame.ttl.toByte())
            .putShort(frame.payload.size.toShort())
            .put(frame.payload)
            .array()
    }
}

/**
 * Deserialises a raw byte array received from a [Link] into a [Frame], or
 * returns `null` if the bytes are malformed, too short, or carry an unknown type tag.
 *
 * All unsigned values stored as signed JVM types (Short, Int) are widened back
 * to their unsigned Kotlin counterparts (UShort, UInt) via [toUShort]/[toUInt].
 */
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
        FRAME_BEACON -> {
            if (raw.size < 9) return null
            Frame.BeaconFrame(
                nextHop = buf.short.toUShort(),
                srcId   = buf.short.toUShort(),
                groupId = buf.int.toUInt()
            )
        }
        FRAME_INVITE -> {
            if (raw.size < 11) return null
            Frame.InviteFrame(
                nextHop = buf.short.toUShort(),
                srcId   = buf.short.toUShort(),
                dstId   = buf.short.toUShort(),
                groupId = buf.int.toUInt()
            )
        }
        FRAME_MULTICAST -> {
            if (raw.size < 12) return null
            val srcId      = buf.short.toUShort()
            val groupId    = buf.int.toUInt()
            val seqNum     = buf.short.toUShort()
            val ttl        = buf.get().toUByte()
            val payloadLen = buf.short.toInt()
            if (raw.size < 12 + payloadLen) return null
            val payload = ByteArray(payloadLen).also { buf.get(it) }
            Frame.MulticastFrame(srcId, groupId, seqNum, ttl, payload)
        }
        else -> null  // Unknown type tag — silently discard.
    }
}
