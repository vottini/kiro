package batman

import java.time.Instant

/**
 * A single entry in the distributed routing table, representing the best known
 * path to a remote originator as learned from received OGMs.
 *
 * BATMAN does not store full paths. Instead, each node only remembers its single
 * best next hop toward each destination — the neighbour that forwarded the OGM
 * with the highest remaining TTL (i.e. via the fewest hops so far).
 *
 * Entries are updated whenever a better OGM arrives (higher [bestTtl]) and
 * considered stale when [lastSeen] exceeds a threshold derived from the link's
 * [Link.ogmInterval]. Stale entries indicate the route is no longer reachable.
 *
 * @property nextHop NodeId of the direct neighbour to forward packets to.
 *   This is the [Ogm.senderId] of the best received OGM for this originator —
 *   not the originator itself, which may be many hops away.
 * @property link The physical [Link] interface on which [nextHop] is reachable.
 *   Packets for this destination must be enqueued on this link's [TxQueue] slot.
 * @property bestTtl The highest TTL value seen in any OGM from this originator.
 *   Higher = fewer hops = better path quality. Used to choose between competing
 *   routes when a new OGM arrives.
 * @property lastSeq Sequence number of the most recently accepted OGM from this
 *   originator. Stored for reference; deduplication is handled by [SeenWindowCache].
 * @property lastSeen Wall-clock time when [bestTtl] was last updated. Used by
 *   the stale-entry eviction logic: if no OGM has been seen for more than
 *   N × [Link.ogmInterval], the route is removed from the table.
 */
data class NeighborEntry(
    val nextHop: NodeId,
    val link: Link,
    val bestTtl: UByte,
    val lastSeq: UShort,
    val lastSeen: Instant
)
