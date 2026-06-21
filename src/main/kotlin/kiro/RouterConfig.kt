package kiro

import kotlin.math.roundToInt
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Recommended [KiroRouter] timing parameters for a given link speed.
 *
 * @property ogmInterval How often each node broadcasts an OGM on each link.
 * @property neighborPurgeMultiplier Number of missed OGM cycles before a route is evicted.
 */
data class RouterConfig(
    val ogmInterval: Duration,
    val neighborPurgeMultiplier: Int
) {
    /** Time a route survives without a refreshing OGM before being evicted. */
    val purgeTimeout: Duration get() = ogmInterval * neighborPurgeMultiplier
}

/**
 * Estimates [RouterConfig] parameters for a given link bandwidth, keeping protocol
 * overhead at or below `(1 - dataFraction)` of the channel capacity.
 *
 * ## How it works
 *
 * Each of the [expectedNodes] nodes broadcasts a 6-byte OGM on every link once per
 * [RouterConfig.ogmInterval]. A link therefore carries at most:
 *
 *   N × 6 bytes / ogmInterval  bytes/sec of OGM traffic
 *
 * Solving for the interval that keeps this within the protocol budget:
 *
 *   ogmInterval = N × 48 bits / ((1 − dataFraction) × linkBandwidthBps)
 *
 * [RouterConfig.neighborPurgeMultiplier] is chosen so that the resulting
 * [RouterConfig.purgeTimeout] stays close to 60 s (clamped between 3 and 5 cycles).
 * This keeps failure-detection time reasonable across all link speeds without
 * waiting too long on slow links or evicting too aggressively on fast ones.
 *
 * ## Caveats
 *
 * - [expectedNodes] should be a conservative upper bound on the number of nodes the
 *   link will ever need to carry OGMs for. Underestimating it will leave less than
 *   [dataFraction] of bandwidth for application data once the network grows.
 * - Beacon frames (9 bytes, for multicast tree maintenance) add a small additional
 *   overhead proportional to the number of active multicast groups per link.
 *   For networks with many concurrent multicast groups, reduce [dataFraction] slightly.
 * - The returned [RouterConfig.ogmInterval] is floored at [minOgmInterval]. Below that
 *   threshold, scheduling jitter dominates and the interval loses meaning. Defaults to
 *   5 s — aggressive enough for most radio meshes while avoiding unnecessary chatter
 *   on very fast links.
 *
 * @param linkBandwidthBps Link capacity in bits per second.
 * @param expectedNodes Conservative upper bound on network size. Use the largest
 *   node count you expect this link to serve. More nodes → longer interval.
 * @param dataFraction Minimum fraction of bandwidth to reserve for application data.
 *   Defaults to 0.5 (50 %).
 * @param minOgmInterval Lower bound on the computed interval. Defaults to 5 s.
 */
fun recommendedConfig(
    linkBandwidthBps: Long,
    expectedNodes: Int = 40,
    dataFraction: Double = 0.5,
    minOgmInterval: Duration = 5.seconds
): RouterConfig {
    require(linkBandwidthBps > 0) { "linkBandwidthBps must be positive" }
    require(expectedNodes > 0) { "expectedNodes must be positive" }
    require(dataFraction in 0.0..1.0) { "dataFraction must be between 0 and 1" }
    require(minOgmInterval.isPositive()) { "minOgmInterval must be positive" }

    // N × 48 bits per OGM cycle must fit in the protocol slice of the channel.
    val protocolBudgetBps = linkBandwidthBps * (1.0 - dataFraction)
    val intervalSeconds   = (expectedNodes * 48.0) / protocolBudgetBps
    val ogmInterval       = intervalSeconds.seconds.coerceAtLeast(minOgmInterval)

    // Target a purge window of ~60 s so that node failures are detected within
    // a minute on any link speed. Clamped to [3, 5] missed cycles.
    val purgeMult = (60.seconds / ogmInterval).roundToInt().coerceIn(3, 5)

    return RouterConfig(ogmInterval, purgeMult)
}
