package batman

/**
 * Wire-format type tags (4-bit field; values 0–4, leaving 5–15 reserved).
 *
 * Stored in the upper nibble of byte 0, which is shared across all frame types:
 *   B0 = [type:4 | firstId[11:8]:4]
 *
 * "firstId" is whichever 12-bit identifier is most natural for each frame
 * type (origId for OGM, nextHop for unicast frames, srcId for MulticastFrame).
 *
 * The link itself determines whether frames are authenticated or encrypted —
 * no per-frame flag is needed.
 */
private const val TYPE_OGM       = 0
private const val TYPE_DATA      = 1
private const val TYPE_BEACON    = 2
private const val TYPE_MULTICAST = 3
private const val TYPE_INVITE    = 4

/**
 * Serialises a [Frame] into the compact bit-packed wire format.
 *
 * ## Bit-packing rationale
 *
 * NodeIds are capped at 12 bits (max node 4095) and TTL at 4 bits (max 15
 * hops), saving one byte per field compared with a naïve 16-bit layout. The
 * frame type uses 4 bits (upper nibble of byte 0), supporting up to 16 distinct
 * frame types; the lower nibble of byte 0 carries the high 4 bits of the first
 * identifier.
 *
 * Authentication or encryption is a link-level property: every frame on a
 * given [Link] is always in the same mode, so no per-frame flag is required.
 *
 * ## Wire layouts (sizes in bytes; all multi-byte integers big-endian)
 *
 * ```
 * OGM       6    B0=[type:4|origId[11:8]:4]       B1=origId[7:0]
 *                B2=[sendId[11:8]:4|ttl[3:0]:4]    B3=sendId[7:0]
 *                B4=seqNum[15:8]                    B5=seqNum[7:0]
 *
 * DATA      7+n  B0=[type:4|nextHop[11:8]:4]       B1=nextHop[7:0]
 *                B2=[srcId[11:8]:4|dstId[11:8]:4]  B3=srcId[7:0]   B4=dstId[7:0]
 *                B5=[ttl[3:0]:4|spare:4]            B6=payloadLen(8)
 *                payload[0..n-1]
 *
 * BEACON    9    B0=[type:4|nextHop[11:8]:4]         B1=nextHop[7:0]
 *                B2=[srcId[11:8]:4|owner[11:8]:4]    B3=srcId[7:0]   B4=owner[7:0]
 *                B5=gseq[15:8]                        B6=gseq[7:0]
 *                B7=[activeRoot[11:8]:4|spare:4]      B8=activeRoot[7:0]
 *
 * MULTICAST 9+n  B0=[type:4|srcId[11:8]:4]           B1=srcId[7:0]
 *                B2=[owner[11:8]:4|ttl[3:0]:4]        B3=owner[7:0]
 *                B4=gseq[15:8]                        B5=gseq[7:0]
 *                B6=mcastSeq[15:8]                    B7=mcastSeq[7:0]
 *                B8=payloadLen(8)
 *                payload[0..n-1]
 *
 * INVITE    9+2d B0=[type:4|nextHop[11:8]:4]         B1=nextHop[7:0]
 *                B2=[srcId[11:8]:4|dstId[11:8]:4]    B3=srcId[7:0]   B4=dstId[7:0]
 *                B5=[owner[11:8]:4|deputyCount:4]     B6=owner[7:0]   (spare nibble = count)
 *                B7=gseq[15:8]                        B8=gseq[7:0]
 *                per deputy i: [dep[11:8]:4|spare:4], dep[7:0]        (2 bytes × d deputies)
 * ```
 *
 * BEACON and INVITE encode the [GroupId] as its constituent owner (12-bit) and
 * group-sequence (16-bit) fields rather than the raw 32-bit UInt, because the
 * owner already fits in 12 bits — saving 4 bytes versus a flat 32-bit encoding.
 */
fun encode(frame: Frame): ByteArray = when (frame) {
    is Frame.OgmFrame       -> encodeOgm(frame)
    is Frame.DataFrame      -> encodeData(frame)
    is Frame.BeaconFrame    -> encodeBeacon(frame)
    is Frame.MulticastFrame -> encodeMulticast(frame)
    is Frame.InviteFrame    -> encodeInvite(frame)
}

/**
 * OGM: 6 bytes.
 *
 * B0: [type:4|origId[11:8]:4]
 * B1: origId[7:0]
 * B2: [sendId[11:8]:4|ttl[3:0]:4]
 * B3: sendId[7:0]
 * B4: seqNum[15:8]
 * B5: seqNum[7:0]
 */
private fun encodeOgm(frame: Frame.OgmFrame): ByteArray {
    val ogm    = frame.ogm
    val origId = ogm.originatorId.toInt() and 0xFFF
    val sendId = ogm.senderId.toInt()     and 0xFFF
    val ttl    = ogm.ttl.toInt()          and 0xF
    val seq    = ogm.seqNum.toInt()       and 0xFFFF
    return byteArrayOf(
        ((TYPE_OGM shl 4) or (origId ushr 8)).toByte(),
        (origId and 0xFF).toByte(),
        ((sendId ushr 8 shl 4) or ttl).toByte(),
        (sendId and 0xFF).toByte(),
        (seq ushr 8).toByte(),
        (seq and 0xFF).toByte()
    )
}

/**
 * DATA: 7 + payload bytes.
 *
 * B0: [type:4|nextHop[11:8]:4]
 * B1: nextHop[7:0]
 * B2: [srcId[11:8]:4|dstId[11:8]:4]
 * B3: srcId[7:0]
 * B4: dstId[7:0]
 * B5: [ttl:4|spare:4]  (spare bits are zero)
 * B6: payloadLen (unsigned, 0–255)
 * B7…: payload
 */
private fun encodeData(frame: Frame.DataFrame): ByteArray {
    val nextHop = frame.nextHop.toInt() and 0xFFF
    val srcId   = frame.srcId.toInt()   and 0xFFF
    val dstId   = frame.dstId.toInt()   and 0xFFF
    val ttl     = frame.ttl.toInt()     and 0xF
    val payload = frame.payload
    return ByteArray(7 + payload.size).also { b ->
        b[0] = ((TYPE_DATA shl 4) or (nextHop ushr 8)).toByte()
        b[1] = (nextHop and 0xFF).toByte()
        b[2] = ((srcId ushr 8 shl 4) or (dstId ushr 8)).toByte()
        b[3] = (srcId and 0xFF).toByte()
        b[4] = (dstId and 0xFF).toByte()
        b[5] = (ttl shl 4).toByte()
        b[6] = payload.size.toByte()
        payload.copyInto(b, destinationOffset = 7)
    }
}

/**
 * BEACON: 9 bytes.
 *
 * GroupId is unpacked into owner (12-bit) and group-sequence (16-bit).
 * [Frame.BeaconFrame.activeRoot] occupies B7–B8: relay nodes stop forwarding
 * when their own NodeId matches activeRoot, without needing local group state.
 *
 * B0: [type:4|nextHop[11:8]:4]
 * B1: nextHop[7:0]
 * B2: [srcId[11:8]:4|owner[11:8]:4]
 * B3: srcId[7:0]
 * B4: owner[7:0]
 * B5: gseq[15:8]
 * B6: gseq[7:0]
 * B7: [activeRoot[11:8]:4|spare:4]
 * B8: activeRoot[7:0]
 */
private fun encodeBeacon(frame: Frame.BeaconFrame): ByteArray {
    val nextHop    = frame.nextHop.toInt()       and 0xFFF
    val srcId      = frame.srcId.toInt()         and 0xFFF
    val owner      = frame.groupId.owner.toInt() and 0xFFF
    val gseq       = frame.groupId.seq.toInt()   and 0xFFFF
    val activeRoot = frame.activeRoot.toInt()     and 0xFFF
    return byteArrayOf(
        ((TYPE_BEACON shl 4) or (nextHop ushr 8)).toByte(),
        (nextHop and 0xFF).toByte(),
        ((srcId ushr 8 shl 4) or (owner ushr 8)).toByte(),
        (srcId and 0xFF).toByte(),
        (owner and 0xFF).toByte(),
        (gseq ushr 8).toByte(),
        (gseq and 0xFF).toByte(),
        ((activeRoot ushr 8) shl 4).toByte(),
        (activeRoot and 0xFF).toByte()
    )
}

/**
 * MULTICAST: 9 + payload bytes.
 *
 * B0: [type:4|srcId[11:8]:4]
 * B1: srcId[7:0]
 * B2: [owner[11:8]:4|ttl[3:0]:4]
 * B3: owner[7:0]
 * B4: gseq[15:8]
 * B5: gseq[7:0]
 * B6: mcastSeq[15:8]
 * B7: mcastSeq[7:0]
 * B8: payloadLen (unsigned, 0–255)
 * B9…: payload
 */
private fun encodeMulticast(frame: Frame.MulticastFrame): ByteArray {
    val srcId    = frame.srcId.toInt()           and 0xFFF
    val owner    = frame.groupId.owner.toInt() and 0xFFF
    val ttl      = frame.ttl.toInt()           and 0xF
    val gseq     = frame.groupId.seq.toInt()   and 0xFFFF
    val mcastSeq = frame.seqNum.toInt()          and 0xFFFF
    val payload  = frame.payload
    return ByteArray(9 + payload.size).also { b ->
        b[0] = ((TYPE_MULTICAST shl 4) or (srcId ushr 8)).toByte()
        b[1] = (srcId and 0xFF).toByte()
        b[2] = ((owner ushr 8 shl 4) or ttl).toByte()
        b[3] = (owner and 0xFF).toByte()
        b[4] = (gseq ushr 8).toByte()
        b[5] = (gseq and 0xFF).toByte()
        b[6] = (mcastSeq ushr 8).toByte()
        b[7] = (mcastSeq and 0xFF).toByte()
        b[8] = payload.size.toByte()
        payload.copyInto(b, destinationOffset = 9)
    }
}

/**
 * INVITE: 9 + 2×d bytes (d = number of deputies, 0–15).
 *
 * The spare nibble in B5 is repurposed for the deputy count, so frames with
 * zero deputies remain 9 bytes — identical to the previous layout.
 *
 * B0: [type:4|nextHop[11:8]:4]
 * B1: nextHop[7:0]
 * B2: [srcId[11:8]:4|dstId[11:8]:4]
 * B3: srcId[7:0]
 * B4: dstId[7:0]
 * B5: [owner[11:8]:4|deputyCount:4]
 * B6: owner[7:0]
 * B7: gseq[15:8]
 * B8: gseq[7:0]
 * per deputy i: [dep[11:8]:4|spare:4], dep[7:0]
 */
private fun encodeInvite(frame: Frame.InviteFrame): ByteArray {
    val nextHop  = frame.nextHop.toInt()         and 0xFFF
    val srcId    = frame.srcId.toInt()           and 0xFFF
    val dstId    = frame.dstId.toInt()           and 0xFFF
    val owner    = frame.groupId.owner.toInt()   and 0xFFF
    val gseq     = frame.groupId.seq.toInt()     and 0xFFFF
    val deputies = frame.deputies
    return ByteArray(9 + 2 * deputies.size).also { b ->
        b[0] = ((TYPE_INVITE shl 4) or (nextHop ushr 8)).toByte()
        b[1] = (nextHop and 0xFF).toByte()
        b[2] = ((srcId ushr 8 shl 4) or (dstId ushr 8)).toByte()
        b[3] = (srcId and 0xFF).toByte()
        b[4] = (dstId and 0xFF).toByte()
        b[5] = ((owner ushr 8 shl 4) or (deputies.size and 0xF)).toByte()
        b[6] = (owner and 0xFF).toByte()
        b[7] = (gseq ushr 8).toByte()
        b[8] = (gseq and 0xFF).toByte()
        deputies.forEachIndexed { i, dep ->
            val d = dep.toInt() and 0xFFF
            b[9 + 2 * i]     = ((d ushr 8) shl 4).toByte()
            b[9 + 2 * i + 1] = (d and 0xFF).toByte()
        }
    }
}

/**
 * Deserialises a raw byte array received from a [Link] into a [Frame].
 *
 * Returns `null` when:
 * - [raw] is empty or shorter than the minimum for the detected frame type.
 * - The type nibble carries a reserved value (5–15).
 * - A DATA or MULTICAST frame declares a payload length that extends beyond
 *   the received bytes.
 *
 * If the link operates in encrypted mode the caller is responsible for
 * decrypting the bytes before passing them here.
 */
fun decode(raw: ByteArray): Frame? {
    if (raw.isEmpty()) return null
    val b0   = raw[0].toInt() and 0xFF
    val type = b0 ushr 4
    return when (type) {
        TYPE_OGM       -> decodeOgm(raw, b0)
        TYPE_DATA      -> decodeData(raw, b0)
        TYPE_BEACON    -> decodeBeacon(raw, b0)
        TYPE_MULTICAST -> decodeMulticast(raw, b0)
        TYPE_INVITE    -> decodeInvite(raw, b0)
        else -> null
    }
}

private fun decodeOgm(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 6) return null
    val origId = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2     = raw[2].toInt() and 0xFF
    val sendId = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val ttl    = (b2 and 0xF).toUByte()
    val seqNum = (((raw[4].toInt() and 0xFF) shl 8) or (raw[5].toInt() and 0xFF)).toUShort()
    return Frame.OgmFrame(Ogm(
        originatorId = origId.toUShort(),
        senderId     = sendId.toUShort(),
        seqNum       = seqNum,
        ttl          = ttl
    ))
}

private fun decodeData(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 7) return null
    val nextHop    = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2         = raw[2].toInt() and 0xFF
    val srcId      = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val dstId      = ((b2 and 0xF) shl 8) or (raw[4].toInt() and 0xFF)
    val ttl        = ((raw[5].toInt() and 0xFF) ushr 4).toUByte()
    val payloadLen = raw[6].toInt() and 0xFF
    if (raw.size < 7 + payloadLen) return null
    return Frame.DataFrame(
        nextHop = nextHop.toUShort(),
        srcId   = srcId.toUShort(),
        dstId   = dstId.toUShort(),
        ttl     = ttl,
        payload = raw.copyOfRange(7, 7 + payloadLen)
    )
}

private fun decodeBeacon(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 9) return null
    val nextHop    = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2         = raw[2].toInt() and 0xFF
    val srcId      = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val owner      = ((b2 and 0xF) shl 8) or (raw[4].toInt() and 0xFF)
    val gseq       = (((raw[5].toInt() and 0xFF) shl 8) or (raw[6].toInt() and 0xFF)).toUShort()
    val activeRoot = (((raw[7].toInt() and 0xFF) ushr 4) shl 8) or (raw[8].toInt() and 0xFF)
    return Frame.BeaconFrame(
        nextHop    = nextHop.toUShort(),
        srcId      = srcId.toUShort(),
        groupId    = GroupId(owner.toUShort(), gseq),
        activeRoot = activeRoot.toUShort()
    )
}

private fun decodeMulticast(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 9) return null
    val srcId      = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2         = raw[2].toInt() and 0xFF
    val owner      = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val ttl        = (b2 and 0xF).toUByte()
    val gseq       = (((raw[4].toInt() and 0xFF) shl 8) or (raw[5].toInt() and 0xFF)).toUShort()
    val mcastSeq   = (((raw[6].toInt() and 0xFF) shl 8) or (raw[7].toInt() and 0xFF)).toUShort()
    val payloadLen = raw[8].toInt() and 0xFF
    if (raw.size < 9 + payloadLen) return null
    return Frame.MulticastFrame(
        srcId   = srcId.toUShort(),
        groupId = GroupId(owner.toUShort(), gseq),
        seqNum  = mcastSeq,
        ttl     = ttl,
        payload = raw.copyOfRange(9, 9 + payloadLen)
    )
}

private fun decodeInvite(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 9) return null
    val nextHop      = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2           = raw[2].toInt() and 0xFF
    val srcId        = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val dstId        = ((b2 and 0xF) shl 8) or (raw[4].toInt() and 0xFF)
    val b5           = raw[5].toInt() and 0xFF
    val owner        = ((b5 ushr 4) shl 8) or (raw[6].toInt() and 0xFF)
    val deputyCount  = b5 and 0xF
    val gseq         = (((raw[7].toInt() and 0xFF) shl 8) or (raw[8].toInt() and 0xFF)).toUShort()
    if (raw.size < 9 + 2 * deputyCount) return null
    val deputies = List(deputyCount) { i ->
        val hi = (raw[9 + 2 * i].toInt() and 0xFF) ushr 4
        val lo = raw[9 + 2 * i + 1].toInt() and 0xFF
        ((hi shl 8) or lo).toUShort()
    }
    return Frame.InviteFrame(
        nextHop  = nextHop.toUShort(),
        srcId    = srcId.toUShort(),
        dstId    = dstId.toUShort(),
        groupId  = GroupId(owner.toUShort(), gseq),
        deputies = deputies
    )
}
