package systems.untangle.kiro

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
     */
    val ogmInterval: Duration

    /**
     * Physical capacity of this link in bits per second.
     *
     * Used as the primary routing metric: each OGM carries the minimum bandwidth
     * tier seen across all links it has traversed. The router prefers the path
     * whose bottleneck link is widest — a 3-hop all-WiFi path beats a 2-hop
     * WiFi+LoRa path when the LoRa link is the bottleneck.
     *
     * The value is converted to a 6-bit log₂ tier ([bandwidthTier]) before being
     * placed on the wire, so only powers-of-two distinctions matter for routing.
     * Representative values: LoRa 50 bps → tier 5; LoRa 50 kbps → tier 15;
     * WiFi 100 Mbps → tier 26; Ethernet 1 Gbps → tier 29.
     */
    val bandwidthBps: Long

    /**
     * Broadcasts [frame] to all nodes reachable on this medium.
     * Suspends until the frame has been handed off to the underlying transport;
     * does not guarantee delivery. Implementations should be backpressure-aware
     * so that the [TxQueue] pull model works correctly.
     */
    suspend fun broadcast(frame: ByteArray)

    /**
     * Cold flow of raw frames received on this link.
     * Each collector (one per link, started by [KiroRouter]) receives every
     * frame that arrives on the medium, regardless of intended recipient —
     * the router inspects the nextHop field to decide whether to process or drop.
     */
    val frames: Flow<ByteArray>
}

/**
 * Converts a raw bandwidth in bits-per-second to a 6-bit log₂ tier (0–63).
 *
 * `tier = floor(log₂(bps))`, so each tier represents a 2× bandwidth step.
 * Two links in the same tier are treated as equivalent for routing purposes.
 * The tier fits in 6 bits, covering 1 bps (tier 0) through ~9 Pbps (tier 63).
 */
fun bandwidthTier(bps: Long): UByte =
    if (bps <= 1L) 0u
    else kotlin.math.log2(bps.toDouble()).toInt().coerceIn(0, 63).toUByte()

/** Pre-computed [bandwidthTier] for this link's [Link.bandwidthBps]. */
val Link.bandwidthTier: UByte get() = bandwidthTier(bandwidthBps)
