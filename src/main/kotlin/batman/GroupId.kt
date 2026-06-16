package batman

/**
 * A 32-bit multicast group identifier, globally unique within the mesh.
 *
 * The identifier is constructed by packing two 16-bit fields into a single UInt:
 *
 *   bits 31–16 : [NodeId] of the group owner (the node that called [BatmanRouter.createGroup])
 *   bits 15–0  : per-owner sequential counter, incremented for each new group
 *
 * This design guarantees uniqueness without any coordination between nodes —
 * each node's groups are namespaced under its own [NodeId]. The owner field is
 * also directly extractable at zero cost, which is required by [BatmanRouter]
 * to route [Frame.BeaconFrame]s toward the correct destination.
 */
typealias GroupId = UInt

/**
 * Constructs a [GroupId] from an [owner] node and a per-owner [seq]uence number.
 * The owner occupies the high 16 bits; the sequence occupies the low 16 bits.
 */
fun groupId(owner: NodeId, seq: UShort): GroupId = (owner.toUInt() shl 16) or seq.toUInt()

/** Extracts the [NodeId] of the group owner from a [GroupId]. */
fun GroupId.owner(): NodeId = (this shr 16).toUShort()

/** Extracts the per-owner sequence number from a [GroupId]. */
fun GroupId.seq(): UShort = (this and 0xFFFFu).toUShort()
