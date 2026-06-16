package batman

import java.time.Instant

data class NeighborEntry(
    val nextHop: NodeId,
    val link: Link,
    val bestTtl: UByte,
    val lastSeq: UShort,
    val lastSeen: Instant
)
