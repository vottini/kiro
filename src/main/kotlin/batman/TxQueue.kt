package batman

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Describes the kind of frame carried by a [TxEntry].
 *
 * Flavors are used by [Rule]s to determine transmission ordering. The
 * distinction between control and data flavors is the basis of the
 * [controlFirst] built-in rule, but callers can define arbitrary rules that
 * treat individual flavors differently (e.g. rank beacons above OGMs).
 *
 * [isControl] groups the network-management flavors (OGM, BEACON, INVITE)
 * that must be delivered promptly to keep routing state fresh, as opposed to
 * application-data flavors (DATA, MULTICAST) that can tolerate queueing.
 */
enum class PacketFlavor {
    OGM, BEACON, INVITE, MULTICAST, DATA;

    val isControl: Boolean get() = this == OGM || this == BEACON || this == INVITE
}

/**
 * A [Rule] is a [Comparator] over [TxEntry] that expresses one ordering
 * concern. Rules are composed in priority order by [TxQueue]: the first rule
 * that returns a non-zero comparison wins; later rules act as tiebreakers.
 *
 * Built-in rules: [controlFirst], [olderFirst], [insertionOrderFirst].
 */
typealias Rule = Comparator<TxEntry>

/**
 * Control flavors (OGM, BEACON, INVITE) are transmitted before data flavors
 * (DATA, MULTICAST) so that routing state stays fresh under congestion.
 */
val controlFirst: Rule = compareBy { if (it.flavor.isControl) 0 else 1 }

/**
 * Among entries with the same effective priority, older frames are sent first,
 * providing fair FIFO behaviour within a priority class.
 */
val olderFirst: Rule = compareBy { it.enqueuedAt }

/**
 * Globally unique tie-breaker. Guarantees a strict total order in the
 * [TreeSet], which is required to prevent silent entry loss when two entries
 * compare equal on all other fields.
 */
val insertionOrderFirst: Rule = compareBy { it.insertionOrder }

/**
 * The default rule list applied when no custom rules are supplied to [TxQueue].
 * Applies: control before data → older before newer → insertion order.
 */
fun defaultRules(): List<Rule> = listOf(controlFirst, olderFirst, insertionOrderFirst)

/**
 * Global monotonically increasing counter that assigns a unique insertion order
 * to every [TxEntry], used by [insertionOrderFirst].
 */
private val insertionCounter = AtomicLong(0)

/**
 * A single item waiting to be transmitted on one of its [eligibleLinks].
 *
 * @property frame Raw bytes to hand to [Link.broadcast].
 * @property eligibleLinks The set of [Link]s on which this frame may be sent.
 *   The first link whose TX coroutine calls [TxQueue.pollFor] will claim and
 *   transmit it. For frames that must go on a specific link (OGMs, unicast
 *   hops) use a singleton set; for frames that can go on any of several
 *   equivalent links leave the set open so the fastest available link wins.
 * @property flavor Describes the kind of frame, used by [Rule]s to determine
 *   ordering relative to other entries in the queue.
 * @property enqueuedAt Wall-clock time of enqueue, used by [olderFirst].
 * @property insertionOrder Globally unique tie-breaker, used by [insertionOrderFirst].
 */
data class TxEntry(
    val frame: ByteArray,
    val eligibleLinks: Set<Link>,
    val flavor: PacketFlavor,
    val enqueuedAt: Instant = Instant.now(),
    val insertionOrder: Long = insertionCounter.getAndIncrement()
) {
    // Identity is defined solely by insertionOrder — each TxEntry is unique.
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TxEntry) return false
        return insertionOrder == other.insertionOrder
    }

    override fun hashCode(): Int = insertionOrder.hashCode()
}

/**
 * Central transmit queue shared by all [Link] TX loops.
 *
 * ## Pull model
 *
 * Instead of pushing frames directly to each link, producers enqueue [TxEntry]
 * objects here and each link's TX coroutine pulls entries when the link becomes
 * available (i.e. the previous [Link.broadcast] call has returned). This means:
 *   - A slow radio never blocks a fast one.
 *   - Backpressure is handled naturally: the link drains at its own pace.
 *   - Ordering is enforced at dequeue time across all pending frames.
 *
 * ## Rule-based ordering
 *
 * The queue is ordered by composing a [List] of [Rule]s into a single
 * [Comparator]. Rules are applied in list order; the first rule that returns
 * a non-zero result determines the relative position of two entries. The
 * default rule list ([defaultRules]) places control frames before data frames,
 * then breaks ties by enqueue time and finally by insertion order.
 *
 * Custom rule lists can reorder, add, or remove rules. For example, to always
 * drain OGMs before beacons before all other control frames:
 * ```
 * val q = TxQueue(listOf(
 *     compareBy { when (it.flavor) { OGM -> 0; BEACON -> 1; else -> 2 } },
 *     olderFirst,
 *     insertionOrderFirst
 * ))
 * ```
 *
 * ## Eligible-link work stealing
 *
 * Each [TxEntry] carries a set of [TxEntry.eligibleLinks] rather than a
 * single target. The first link whose TX coroutine becomes free and calls
 * [pollFor] claims the entry exclusively, enabling natural load-balancing
 * across redundant paths without any explicit scheduler.
 *
 * ## Suspension
 *
 * [pollFor] suspends the calling coroutine until an eligible frame is
 * available. When [enqueue] finds a waiting coroutine for any of the entry's
 * eligible links it completes the deferred directly, bypassing the sorted set.
 *
 * ## Thread safety
 *
 * All mutations to [entries] are guarded by [mutex].
 */
class TxQueue(rules: List<Rule> = defaultRules()) {
    private val mutex = Mutex()

    /** Composite comparator built by chaining [rules] in order. */
    private val comparator: Rule = rules.reduce { acc, rule -> acc.then(rule) }

    /** Sorted set of pending entries, ordered by the composite [comparator]. */
    private val entries = TreeSet<TxEntry>(comparator)

    /**
     * Per-link queues of suspended [pollFor] callers waiting for a frame.
     * When a frame is enqueued whose eligible set includes a link that has a
     * waiting coroutine, the deferred is resolved immediately without touching
     * [entries].
     */
    private val waiters = ConcurrentHashMap<String, ArrayDeque<CompletableDeferred<TxEntry>>>()

    /**
     * Adds [entry] to the queue, or delivers it directly to a waiting [pollFor]
     * caller if one exists for any of the entry's [TxEntry.eligibleLinks].
     *
     * Eligible links are checked in iteration order; the first link with a
     * waiting coroutine claims the entry. If no waiter is found the entry is
     * parked in the sorted set until a link calls [pollFor].
     */
    suspend fun enqueue(entry: TxEntry) = mutex.withLock {
        val waiter = entry.eligibleLinks
            .firstNotNullOfOrNull { link -> waiters[link.id]?.takeIf { it.isNotEmpty() } }
            ?.removeFirst()
        if (waiter != null) {
            waiter.complete(entry)
        } else {
            entries.add(entry)
        }
    }

    /**
     * Returns the highest-priority pending [TxEntry] for which [link] is
     * eligible, suspending the caller until one is available.
     *
     * The deferred is registered under the mutex to eliminate the TOCTOU race
     * between "nothing available" and "enqueue arrives": if a frame is enqueued
     * between the check and the suspension, the enqueue path will find the waiter
     * and complete the deferred before [await] is even called.
     */
    suspend fun pollFor(link: Link): TxEntry {
        val deferred = CompletableDeferred<TxEntry>()
        mutex.withLock {
            val entry = entries.firstOrNull { link in it.eligibleLinks }
            if (entry != null) {
                entries.remove(entry)
                deferred.complete(entry)
            } else {
                waiters.getOrPut(link.id) { ArrayDeque() }.addLast(deferred)
            }
        }
        return deferred.await()
    }
}
