package kiro

/**
 * Wire-format type tags (4-bit field; values 0–3, leaving 4–15 reserved).
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
 *                B5=[ttl[3:0]:4|spare:4]            B6…=varint(payloadLen)
 *                payload[0..n-1]
 *
 * BEACON    8    B0=[type:4|nextHop[11:8]:4]         B1=nextHop[7:0]
 *                B2=[srcId[11:8]:4|gid[19:16]:4]     B3=srcId[7:0]
 *                B4=gid[15:8]                         B5=gid[7:0]
 *                B6=[activeRoot[11:8]:4|spare:4]      B7=activeRoot[7:0]
 *
 * MULTICAST 8+n  B0=[type:4|srcId[11:8]:4]           B1=srcId[7:0]
 *                B2=[gid[19:16]:4|ttl[3:0]:4]         B3=gid[15:8]
 *                B4=gid[7:0]                           B5=mcastSeq[15:8]
 *                B6=mcastSeq[7:0]                      B7…=varint(payloadLen)
 *                payload[0..n-1]
 * ```
 *
 * Payload lengths are encoded as 7-bit continuation varints (little-endian groups of 7 bits,
 * high bit set means another byte follows). Payloads ≤127 bytes cost 1 length byte; ≤16383
 * cost 2; ≤2097151 cost 3. No upper limit is imposed at the codec layer.
 *
 * BEACON and MULTICAST encode [GroupId] as an opaque 20-bit value (bits [19:0] of [GroupId.id]).
 */
fun encode(frame: Frame): ByteArray = when (frame) {
    is Frame.OgmFrame       -> encodeOgm(frame)
    is Frame.DataFrame      -> encodeData(frame)
    is Frame.BeaconFrame    -> encodeBeacon(frame)
    is Frame.MulticastFrame -> encodeMulticast(frame)
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
    val nextHop  = frame.nextHop.toInt() and 0xFFF
    val srcId    = frame.srcId.toInt()   and 0xFFF
    val dstId    = frame.dstId.toInt()   and 0xFFF
    val ttl      = frame.ttl.toInt()     and 0xF
    val payload  = frame.payload
    val lenBytes = encodeVarint(payload.size)
    return ByteArray(6 + lenBytes.size + payload.size).also { b ->
        b[0] = ((TYPE_DATA shl 4) or (nextHop ushr 8)).toByte()
        b[1] = (nextHop and 0xFF).toByte()
        b[2] = ((srcId ushr 8 shl 4) or (dstId ushr 8)).toByte()
        b[3] = (srcId and 0xFF).toByte()
        b[4] = (dstId and 0xFF).toByte()
        b[5] = (ttl shl 4).toByte()
        lenBytes.copyInto(b, destinationOffset = 6)
        payload.copyInto(b, destinationOffset = 6 + lenBytes.size)
    }
}

/**
 * BEACON: 8 bytes.
 *
 * GroupId is encoded as an opaque 20-bit value (gid.id and 0xFFFFF).
 * [Frame.BeaconFrame.activeRoot] occupies B6–B7: relay nodes stop forwarding
 * when their own NodeId matches activeRoot, without needing local group state.
 *
 * B0: [type:4|nextHop[11:8]:4]
 * B1: nextHop[7:0]
 * B2: [srcId[11:8]:4|gid[19:16]:4]
 * B3: srcId[7:0]
 * B4: gid[15:8]
 * B5: gid[7:0]
 * B6: [activeRoot[11:8]:4|spare:4]
 * B7: activeRoot[7:0]
 */
private fun encodeBeacon(frame: Frame.BeaconFrame): ByteArray {
    val nextHop    = frame.nextHop.toInt()       and 0xFFF
    val srcId      = frame.srcId.toInt()         and 0xFFF
    val gid        = frame.groupId.id.toInt()    and 0xFFFFF
    val activeRoot = frame.activeRoot.toInt()     and 0xFFF
    return byteArrayOf(
        ((TYPE_BEACON shl 4) or (nextHop ushr 8)).toByte(),
        (nextHop and 0xFF).toByte(),
        ((srcId ushr 8 shl 4) or (gid ushr 16)).toByte(),
        (srcId and 0xFF).toByte(),
        ((gid ushr 8) and 0xFF).toByte(),
        (gid and 0xFF).toByte(),
        ((activeRoot ushr 8) shl 4).toByte(),
        (activeRoot and 0xFF).toByte()
    )
}

/**
 * MULTICAST: 8 + payload bytes.
 *
 * B0: [type:4|srcId[11:8]:4]
 * B1: srcId[7:0]
 * B2: [gid[19:16]:4|ttl[3:0]:4]
 * B3: gid[15:8]
 * B4: gid[7:0]
 * B5: mcastSeq[15:8]
 * B6: mcastSeq[7:0]
 * B7: payloadLen (unsigned, 0–255)
 * B8…: payload
 */
private fun encodeMulticast(frame: Frame.MulticastFrame): ByteArray {
    val srcId    = frame.srcId.toInt()      and 0xFFF
    val gid      = frame.groupId.id.toInt() and 0xFFFFF
    val ttl      = frame.ttl.toInt()        and 0xF
    val mcastSeq = frame.seqNum.toInt()     and 0xFFFF
    val payload  = frame.payload
    val lenBytes = encodeVarint(payload.size)
    return ByteArray(7 + lenBytes.size + payload.size).also { b ->
        b[0] = ((TYPE_MULTICAST shl 4) or (srcId ushr 8)).toByte()
        b[1] = (srcId and 0xFF).toByte()
        b[2] = ((gid ushr 16 shl 4) or ttl).toByte()
        b[3] = ((gid ushr 8) and 0xFF).toByte()
        b[4] = (gid and 0xFF).toByte()
        b[5] = (mcastSeq ushr 8).toByte()
        b[6] = (mcastSeq and 0xFF).toByte()
        lenBytes.copyInto(b, destinationOffset = 7)
        payload.copyInto(b, destinationOffset = 7 + lenBytes.size)
    }
}

// ── Varint helpers ────────────────────────────────────────────────────────────

private fun encodeVarint(value: Int): ByteArray {
    require(value >= 0)
    return when {
        value < 0x80     -> byteArrayOf(value.toByte())
        value < 0x4000   -> byteArrayOf((0x80 or (value and 0x7F)).toByte(), (value ushr 7).toByte())
        value < 0x200000 -> byteArrayOf(
            (0x80 or (value and 0x7F)).toByte(),
            (0x80 or ((value ushr 7) and 0x7F)).toByte(),
            (value ushr 14).toByte()
        )
        else -> throw IllegalArgumentException("varint payload length too large: $value")
    }
}

/** Returns (decoded value, bytes consumed), or (-1, -1) on malformed/truncated input. */
private fun decodeVarint(raw: ByteArray, offset: Int): Pair<Int, Int> {
    var result = 0
    var shift = 0
    var pos = offset
    while (pos < raw.size) {
        val b = raw[pos++].toInt() and 0xFF
        result = result or ((b and 0x7F) shl shift)
        if (b and 0x80 == 0) return result to (pos - offset)
        shift += 7
        if (shift >= 21) return -1 to -1
    }
    return -1 to -1
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
    val nextHop = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2      = raw[2].toInt() and 0xFF
    val srcId   = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val dstId   = ((b2 and 0xF) shl 8) or (raw[4].toInt() and 0xFF)
    val ttl     = ((raw[5].toInt() and 0xFF) ushr 4).toUByte()
    val (payloadLen, varintSize) = decodeVarint(raw, 6)
    if (varintSize < 0 || raw.size < 6 + varintSize + payloadLen) return null
    return Frame.DataFrame(
        nextHop = nextHop.toUShort(),
        srcId   = srcId.toUShort(),
        dstId   = dstId.toUShort(),
        ttl     = ttl,
        payload = raw.copyOfRange(6 + varintSize, 6 + varintSize + payloadLen)
    )
}

private fun decodeBeacon(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 8) return null
    val nextHop    = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2         = raw[2].toInt() and 0xFF
    val srcId      = ((b2 ushr 4) shl 8) or (raw[3].toInt() and 0xFF)
    val gidHi      = b2 and 0xF
    val gid        = (gidHi shl 16) or ((raw[4].toInt() and 0xFF) shl 8) or (raw[5].toInt() and 0xFF)
    val activeRoot = (((raw[6].toInt() and 0xFF) ushr 4) shl 8) or (raw[7].toInt() and 0xFF)
    return Frame.BeaconFrame(
        nextHop    = nextHop.toUShort(),
        srcId      = srcId.toUShort(),
        groupId    = GroupId(gid.toUInt()),
        activeRoot = activeRoot.toUShort()
    )
}

private fun decodeMulticast(raw: ByteArray, b0: Int): Frame? {
    if (raw.size < 8) return null
    val srcId    = ((b0 and 0xF) shl 8) or (raw[1].toInt() and 0xFF)
    val b2       = raw[2].toInt() and 0xFF
    val gidHi    = b2 ushr 4
    val ttl      = (b2 and 0xF).toUByte()
    val gid      = (gidHi shl 16) or ((raw[3].toInt() and 0xFF) shl 8) or (raw[4].toInt() and 0xFF)
    val mcastSeq = (((raw[5].toInt() and 0xFF) shl 8) or (raw[6].toInt() and 0xFF)).toUShort()
    val (payloadLen, varintSize) = decodeVarint(raw, 7)
    if (varintSize < 0 || raw.size < 7 + varintSize + payloadLen) return null
    return Frame.MulticastFrame(
        srcId   = srcId.toUShort(),
        groupId = GroupId(gid.toUInt()),
        seqNum  = mcastSeq,
        ttl     = ttl,
        payload = raw.copyOfRange(7 + varintSize, 7 + varintSize + payloadLen)
    )
}
