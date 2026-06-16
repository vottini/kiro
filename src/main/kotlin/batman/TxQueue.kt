package batman

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Transmission priority classes.
 *
 * CONTROL frames (OGMs, beacons, invites) are always sent before DATA frames
 * so that routing state remains fresh even when the medium is congested.
 * Within the same priority class, frames are ordered by [TxEntry.enqueuedAt]
 * (oldest first), providing fair FIFO behaviour per class.
 */
enum class TxPriority { CONTROL, DATA }

/**
 * Global monotonically increasing counter that assigns a unique insertion order
 * to every [TxEntry]. This guarantees a strict total order in the [TreeSet]
 * without relying on hashCode() (which is non-deterministic and can collide,
 * causing TreeSet to silently discard entries it considers equal).
 */
private val insertionCounter = AtomicLong(0)

/**
 * A single item waiting to be transmitted on a specific [Link].
 *
 * @property frame Raw bytes to hand to [Link.broadcast].
 * @property targetLink The interface this frame must go out on. Each link's
 *   [BatmanRouter] TX loop independently polls the queue for frames destined
 *   for its own link, so a slow radio never blocks a fast one.
 * @property priority Determines ordering relative to other frames in the queue.
 * @property enqueuedAt Wall-clock time of enqueue, used as secondary sort key so
 *   that among same-priority frames, the oldest is transmitted first.
 * @property insertionOrder Globally unique tie-breaker ensuring a strict total
 *   order even when two frames share the same priority and enqueue timestamp.
 */
data class TxEntry(
    val frame: ByteArray,
    val targetLink: Link,
    val priority: TxPriority,
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
 * Comparator used by the central [TxQueue].
 * Sort order: CONTROL before DATA → older frames before newer → insertion order as tiebreaker.
 * The insertion-order tiebreaker guarantees a strict total order, which is required
 * by [TreeSet] to avoid silent entry loss on equal-comparing elements.
 */
fun defaultComparator(): Comparator<TxEntry> =
    compareBy<TxEntry> { it.priority.ordinal }
        .thenBy { it.enqueuedAt }
        .thenBy { it.insertionOrder }

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
 *   - Priority ordering is enforced at dequeue time across all pending frames.
 *
 * ## Suspension
 *
 * [pollFor] suspends the calling coroutine until a frame destined for its link
 * is available. When [enqueue] finds a waiting coroutine for the target link it
 * completes the deferred directly, bypassing the sorted set entirely.
 *
 * ## Thread safety
 *
 * All mutations to [entries] are guarded by [mutex]. The [waiters] map uses
 * a [ConcurrentHashMap] for safe concurrent access to the per-link waiter queues.
 */
class TxQueue(comparator: Comparator<TxEntry> = defaultComparator()) {
    private val mutex = Mutex()

    /** Sorted set of pending entries, ordered by [defaultComparator]. */
    private val entries = TreeSet<TxEntry>(comparator)

    /**
     * Per-link queues of suspended [pollFor] callers waiting for a frame.
     * When a frame is enqueued for a link that has a waiting coroutine, the
     * deferred is resolved immediately without touching [entries].
     */
    private val waiters = ConcurrentHashMap<String, ArrayDeque<CompletableDeferred<TxEntry>>>()

    /**
     * Adds [entry] to the queue, or delivers it directly to a waiting [pollFor]
     * caller if one exists for the target link.
     *
     * Called from OGM loops, TX relay logic, and data forwarding paths.
     */
    suspend fun enqueue(entry: TxEntry) = mutex.withLock {
        val pending = waiters[entry.targetLink.id]
        if (!pending.isNullOrEmpty()) {
            // A TX coroutine is already suspended waiting for this link — hand off directly.
            pending.removeFirst().complete(entry)
        } else {
            // No waiter; park the entry in the sorted set until the link is ready.
            entries.add(entry)
        }
    }

    /**
     * Returns the highest-priority pending [TxEntry] destined for [link],
     * suspending the caller until one is available.
     *
     * The deferred is registered under the mutex to eliminate the TOCTOU race
     * between "nothing available" and "enqueue arrives": if a frame is enqueued
     * between the check and the suspension, the enqueue path will find the waiter
     * and complete the deferred before [await] is even called.
     */
    suspend fun pollFor(link: Link): TxEntry {
        val deferred = CompletableDeferred<TxEntry>()
        mutex.withLock {
            val entry = entries.firstOrNull { it.targetLink === link }
            if (entry != null) {
                // Frame already waiting — complete immediately without suspending.
                entries.remove(entry)
                deferred.complete(entry)
            } else {
                // Nothing available — register as a waiter so enqueue can wake us.
                waiters.getOrPut(link.id) { ArrayDeque() }.addLast(deferred)
            }
        }
        return deferred.await()
    }
}
