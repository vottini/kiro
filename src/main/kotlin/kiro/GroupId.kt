package kiro

/**
 * Opaque multicast group identifier. The library treats this as an uninterpreted
 * 20-bit unsigned integer. A common application convention is to pack a 12-bit
 * node id in the upper bits and an 8-bit sequence in the lower bits:
 *   GroupId((nodeId.toUInt() shl 8) or seq.toUInt())
 * but the library does not enforce or decode this layout.
 */
@JvmInline
value class GroupId(val id: UInt)
