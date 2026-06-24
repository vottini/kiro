package systems.untangle.kiro

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import java.time.Instant

/**
 * Coarse health status for a routing table entry, derived from how recently an
 * OGM was heard relative to the link's expected heartbeat interval.
 *
 * - [GOOD]    — an OGM arrived within the last [Link.ogmInterval]; the path is fresh.
 * - [WARNING] — at least one OGM cycle was missed; the path may be degrading.
 *   The entry is still present because the purge multiplier has not been exceeded yet.
 *
 * The entry disappears from the map entirely once the purge threshold is reached,
 * so callers never observe a third "dead" state — absence from the map is that signal.
 */
enum class LinkStatus { GOOD, WARNING }

/**
 * A distilled view of a routing table entry that omits ephemeral bookkeeping fields
 * ([NeighborEntry.lastSeen], [NeighborEntry.lastSeq]) and replaces the raw timestamp
 * with a coarse [LinkStatus] computed at observation time.
 *
 * Equality is purely structural, making it safe to use with [distinctUntilChanged]:
 * two summaries are equal when the route (nextHop, link, quality) and health status
 * are all identical, regardless of which OGM cycle updated the underlying entry.
 *
 * @property nextHop NodeId of the direct neighbour to forward packets to.
 * @property linkId Stable string identifier of the outgoing [Link].
 * @property minBandwidthTier Widest-bottleneck bandwidth tier of the best-known path.
 *   Higher is better (log₂ bits/sec of the narrowest link along the path).
 * @property status Coarse health based on how recently an OGM was heard.
 */
data class RouteSummary(
    val nextHop: NodeId,
    val linkId: String,
    val minBandwidthTier: UByte,
    val status: LinkStatus,
)

/**
 * Transforms a raw routing-table flow into a debounced summary flow that only emits
 * when something meaningful changes.
 *
 * Each upstream emission maps every [NeighborEntry] to a [RouteSummary]:
 *  - [NeighborEntry.lastSeq] is dropped entirely (changes on every OGM, carries no
 *    routing-decision value for observers).
 *  - [NeighborEntry.lastSeen] is converted to a [LinkStatus] by comparing the entry's
 *    age to [Link.ogmInterval]: within one interval → [LinkStatus.GOOD]; beyond → [LinkStatus.WARNING].
 *  - All other fields ([NeighborEntry.nextHop], [NeighborEntry.link],
 *    [NeighborEntry.minBandwidthTier]) are preserved verbatim (link by its stable [Link.id]).
 *
 * [distinctUntilChanged] then suppresses the emission if the resulting map is identical
 * to the previous one — which is the common case when only [lastSeen] was refreshed
 * within the same [LinkStatus] bucket (i.e. consecutive OGMs from a healthy node).
 *
 * Usage:
 * ```kotlin
 * router.routes
 *     .asRouteSummaryFlow()
 *     .collect { table -> updateUi(table) }
 * ```
 */
fun Flow<Map<NodeId, NeighborEntry>>.asRouteSummaryFlow(): Flow<Map<NodeId, RouteSummary>> =
    map { table ->
        val now = Instant.now()
        table.mapValues { (_, entry) ->
            val ageMs = java.time.Duration.between(entry.lastSeen, now).toMillis()
            val status = if (ageMs <= entry.link.ogmInterval.inWholeMilliseconds) LinkStatus.GOOD else LinkStatus.WARNING
            RouteSummary(entry.nextHop, entry.link.id, entry.minBandwidthTier, status)
        }
    }.distinctUntilChanged()
