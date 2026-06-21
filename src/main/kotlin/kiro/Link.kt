package kiro

import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration

/**
 * A 16-bit unsigned node identifier, unique within the mesh network.
 * Kept small (2 bytes) to minimise OGM frame size on band-limited radios.
 */
typealias NodeId = UShort

/**
 * Abstraction over a single broadcast radio interface.
 *
 * Every physical radio (serial, UDP, LoRa, etc.) is modelled as a Link.
 * All nodes within radio range share the same broadcast domain: every frame
 * sent via [broadcast] is received by all reachable neighbours on that medium.
 *
 * The [ogmInterval] drives two interrelated behaviours:
 *   1. How often this node emits its own OGMs on this link (the heartbeat rate).
 *   2. The jitter window used when relaying foreign OGMs — a fraction of this
 *      interval — so slow radios automatically get a longer suppression window.
 *
 * Implementations must be safe to call from multiple coroutines concurrently.
 */
interface Link {
    /** Unique identifier for this interface, used as a map key throughout the router. */
    val id: String

    /**
     * How often this node broadcasts its own OGM on this link.
     * Slower (band-limited) links use longer intervals to reduce medium utilisation.
     * The interval also acts as an implicit routing metric: routes through faster
     * links deliver OGMs more frequently and therefore win in the neighbour table.
     */
    val ogmInterval: Duration

    /**
     * Broadcasts [frame] to all nodes reachable on this medium.
     * Suspends until the frame has been handed off to the underlying transport;
     * does not guarantee delivery. Implementations should be backpressure-aware
     * so that the [TxQueue] pull model works correctly.
     */
    suspend fun broadcast(frame: ByteArray)

    /**
     * Cold flow of raw frames received on this link.
     * Each collector (one per link, started by [BatmanRouter]) receives every
     * frame that arrives on the medium, regardless of intended recipient —
     * the router inspects the nextHop field to decide whether to process or drop.
     */
    val frames: Flow<ByteArray>
}
