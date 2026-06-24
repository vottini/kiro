package systems.untangle.kiro

import java.time.Instant

/**
 * A single entry in the distributed routing table, representing the best known
 * path to a remote originator as learned from received OGMs.
 *
 * BATMAN does not store full paths. Instead, each node only remembers its single
 * best next hop toward each destination — the neighbour that forwarded the OGM
 * with the highest [minBandwidthTier] (widest bottleneck path).
 *
 * @property nextHop NodeId of the direct neighbour to forward packets to.
 *   This is the [Ogm.senderId] of the best received OGM for this originator.
 * @property link The physical [Link] interface on which [nextHop] is reachable.
 *   Packets for this destination must be enqueued on this link's TX slot.
 * @property minBandwidthTier Primary routing metric: the log₂ bandwidth tier of the
 *   narrowest link along the best-known path to this originator. Higher is better.
 *   See [bandwidthTier].
 * @property bestTtl Tiebreaker: the highest TTL seen among OGMs that also carried
 *   [minBandwidthTier]. Higher TTL means fewer hops, preferred when bandwidth is equal.
 * @property lastSeq Sequence number of the most recently accepted OGM. Stored for
 *   reference; deduplication is handled by [SeenWindowCache].
 * @property lastSeen Wall-clock time of the last OGM that refreshed this entry. Used
 *   by the stale-entry eviction logic: entries older than N × [Link.ogmInterval] are
 *   removed from the table.
 */
data class NeighborEntry(
    val nextHop: NodeId,
    val link: Link,
    val minBandwidthTier: UByte,
    val bestTtl: UByte,
    val lastSeq: UShort,
    val lastSeen: Instant,
)
