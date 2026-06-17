package batman

/**
 * A multicast group identifier, globally unique within the mesh.
 *
 * Uniqueness is guaranteed without coordination: each node's groups are
 * namespaced under its own [owner] [NodeId], and [seq] is a per-owner
 * counter incremented each time [BatmanRouter.createGroup] is called.
 *
 * On the wire the two fields are packed into 28 bits (owner 12 bits,
 * seq 16 bits); see [encode]/[decode] in Codec.kt for the layout.
 */
data class GroupId(val owner: NodeId, val seq: UShort)
