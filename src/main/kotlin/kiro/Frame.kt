package kiro

/**
 * An OGM (Originator Message) is the heartbeat of the BATMAN routing protocol.
 * Each node periodically broadcasts OGMs so that every other node in the mesh
 * can discover it and measure path quality.
 *
 * @property originatorId The node that originally created this OGM. Never changes
 *   as the OGM is relayed across hops.
 * @property senderId The node that most recently rebroadcast this OGM. Updated at
 *   every relay hop so neighbours know which direct neighbour forwarded it to them.
 *   Used to populate the neighbour table's nextHop field.
 * @property seqNum Monotonically increasing sequence number per originator.
 *   Recipients use this together with [SeenWindowCache] to detect duplicates and
 *   discard stale relays. Wraps around at UShort.MAX_VALUE (65535).
 * @property ttl Time-to-live; decremented at each relay hop. Prevents OGMs from
 *   circulating indefinitely. Also used as a path-quality metric: higher remaining
 *   TTL on arrival means fewer hops were traversed.
 */
data class Ogm(
    val originatorId: NodeId,
    val senderId: NodeId,
    val seqNum: UShort,
    val ttl: UByte
)

/**
 * Discriminated union of all frame types carried over the mesh.
 *
 * All frames share a single byte type tag in the wire format (see [Codec]).
 * The [KiroRouter] dispatches received frames based on their runtime type.
 */
sealed class Frame {

    /**
     * Wraps an [Ogm] for transmission. Broadcast on all links periodically by
     * each node and relayed (with jitter-based suppression) by intermediate nodes
     * to build and maintain the distributed routing table.
     */
    data class OgmFrame(val ogm: Ogm) : Frame()

    /**
     * Sent periodically by each group member toward the current tree root via unicast
     * routing. As the beacon travels hop-by-hop, every relay node records both the
     * incoming and outgoing links in the [MulticastTree], progressively building a
     * spanning tree rooted at [activeRoot]. This tree is later used to route
     * [MulticastFrame]s efficiently without flooding the entire network.
     *
     * @property nextHop Link-layer next hop for this unicast frame (changes at each relay).
     * @property srcId The group member that originated this beacon (stays constant).
     * @property groupId Identifies which group's tree is being maintained.
     * @property activeRoot The node currently acting as tree root. Relay nodes stop
     *   forwarding the beacon when their own [NodeId] matches this field.
     */
    data class BeaconFrame(
        val nextHop: NodeId,
        val srcId: NodeId,
        val groupId: GroupId,
        val activeRoot: NodeId
    ) : Frame()

    /**
     * A group multicast message, routed along the spanning tree built by [BeaconFrame]s.
     * Any group member or the root can originate a multicast; intermediate nodes
     * forward it only on links registered in the [MulticastTree] for this group,
     * excluding the link it arrived on (to prevent echo).
     *
     * Duplicate suppression uses [SeenWindowCache] keyed on (srcId, seqNum) so that
     * a multicast is delivered and relayed exactly once per node even if it arrives
     * via multiple tree branches simultaneously.
     *
     * @property srcId Node that originated this multicast (stays constant across hops).
     * @property groupId Identifies the destination group.
     * @property seqNum Per-originator sequence number for deduplication.
     * @property ttl Decremented at each relay; prevents infinite loops if the tree
     *   has stale state after a topology change.
     * @property payload Application-level data.
     */
    data class MulticastFrame(
        val srcId: NodeId,
        val groupId: GroupId,
        val seqNum: UShort,
        val ttl: UByte,
        val payload: ByteArray
    ) : Frame() {
        // ByteArray requires manual equals/hashCode because the default uses reference equality.
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is MulticastFrame) return false
            return srcId == other.srcId &&
                groupId == other.groupId &&
                seqNum == other.seqNum &&
                ttl == other.ttl &&
                payload.contentEquals(other.payload)
        }

        override fun hashCode(): Int {
            var result = srcId.hashCode()
            result = 31 * result + groupId.hashCode()
            result = 31 * result + seqNum.hashCode()
            result = 31 * result + ttl.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    /**
     * A unicast data frame routed hop-by-hop through the mesh.
     * Each intermediate node looks up [dstId] in its neighbour table and rewrites
     * [nextHop] to the appropriate next relay before re-enqueueing on the correct link.
     *
     * @property nextHop Link-layer addressee for this hop. Nodes on the broadcast
     *   medium drop frames whose nextHop does not match their own [NodeId].
     * @property srcId Original sender (stays constant across hops).
     * @property dstId Final destination (stays constant across hops).
     * @property ttl Decremented at each hop to bound the worst-case path length.
     * @property payload Application-level data.
     */
    data class DataFrame(
        val nextHop: NodeId,
        val srcId: NodeId,
        val dstId: NodeId,
        val ttl: UByte,
        val payload: ByteArray
    ) : Frame() {
        // ByteArray requires manual equals/hashCode because the default uses reference equality.
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
