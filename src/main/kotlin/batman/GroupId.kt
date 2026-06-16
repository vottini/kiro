package batman

typealias GroupId = UInt

fun groupId(owner: NodeId, seq: UShort): GroupId = (owner.toUInt() shl 16) or seq.toUInt()

fun GroupId.owner(): NodeId = (this shr 16).toUShort()
fun GroupId.seq(): UShort = (this and 0xFFFFu).toUShort()
