package systems.untangle.kiro

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
 * [isControl] groups the network-management flavors (OGM, BEACON)
 * that must be delivered promptly to keep routing state fresh, as opposed to
 * application-data flavors (DATA, MULTICAST) that can tolerate queueing.
 */
enum class PacketFlavor {
    OGM, BEACON, MULTICAST, DATA;

    val isControl: Boolean get() = this == OGM || this == BEACON
}

/**
 * A [Rule] is a [Comparator] over [TxEntry] that expresses one ordering
 * concern. Rules are composed in priority order by [TxQueue]: the first rule
 * that returns a non-zero comparison wins; later rules act as tiebreakers.
 *
 * Built-in rules: [controlFirst], [userPriorityFirst], [olderFirst], [insertionOrderFirst].
 */
typealias Rule = Comparator<TxEntry>

/**
 * Control flavors (OGM, BEACON) are transmitted before data flavors
 * (DATA, MULTICAST) so that routing state stays fresh under congestion.
 */
val controlFirst: Rule = compareBy { if (it.flavor.isControl) 0 else 1 }

/**
 * Client-controlled ordering layer. Lower [TxEntry.userPriority] values are
 * transmitted first. The default value is 0, so entries are unaffected unless
 * the caller explicitly sets [TxEntry.userPriority] or calls [TxHandle.swap].
 */
val userPriorityFirst: Rule = compareBy { it.userPriority }

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
 * Applies: control before data → client priority → older before newer → insertion order.
 */
fun defaultRules(): List<Rule> = listOf(controlFirst, userPriorityFirst, olderFirst, insertionOrderFirst)

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
 * @property userPriority Client-controlled ordering weight used by [userPriorityFirst].
 *   Lower values are transmitted first; default 0 preserves standard ordering.
 *   Mutated indirectly via [TxHandle.swap].
 */
data class TxEntry(
    val frame: ByteArray,
    val eligibleLinks: Set<Link>,
    val flavor: PacketFlavor,
    val enqueuedAt: Instant = Instant.now(),
    val insertionOrder: Long = insertionCounter.getAndIncrement(),
    val userPriority: Int = 0
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
 * A handle returned by [TxQueue.enqueue] that lets the caller observe and
 * mutate a queued entry before it is claimed by a link TX loop.
 *
 * All mutation methods ([cancel], [replace], [swap]) are no-ops (returning
 * `false`) once the entry has been claimed for transmission.
 */
class TxHandle internal constructor(
    /** Stable identifier equal to [TxEntry.insertionOrder] at enqueue time. */
    val id: Long,
    private val queue: TxQueue
) {
    internal val sentDeferred = CompletableDeferred<Unit>()

    /** `true` once the entry has been claimed by a link TX loop. */
    val isSent: Boolean get() = sentDeferred.isCompleted

    /**
     * Registers [block] to be called (on an unspecified thread, without
     * suspending) when this entry is transmitted. Safe to call from any
     * context; fires immediately if the entry was already sent.
     */
    fun onSent(block: () -> Unit) {
        sentDeferred.invokeOnCompletion { cause -> if (cause == null) block() }
    }

    /**
     * Suspends until this entry has been claimed by a link TX loop for
     * transmission. Returns immediately if the entry was already sent.
     * Must be called from a coroutine context.
     *
     * Example — run code after transmission in a coroutine:
     * ```
     * launch { handle.awaitSent(); doSomethingAsync() }
     * ```
     */
    suspend fun awaitSent() = sentDeferred.await()

    /**
     * Removes the entry from the queue without transmitting it.
     * Returns `false` if the entry was already transmitted.
     */
    suspend fun cancel(): Boolean = queue.cancel(this)

    /**
     * Replaces the frame bytes while keeping the entry at its current queue
     * position. Returns `false` if the entry was already transmitted.
     */
    suspend fun replace(newFrame: ByteArray): Boolean = queue.replace(this, newFrame)

    /**
     * Swaps this entry's queue position with [other] by exchanging their
     * [TxEntry.userPriority] and [TxEntry.enqueuedAt] ordering keys. After the
     * call, this entry occupies [other]'s former position and vice versa.
     *
     * Note: [controlFirst] is always applied before the swapped keys, so a
     * control frame and a data frame cannot exchange positions this way.
     *
     * Returns `false` if either entry was already transmitted.
     */
    suspend fun swap(other: TxHandle): Boolean = queue.swap(this, other)
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
 * then applies client priority, then breaks ties by enqueue time and insertion order.
 *
 * ## Eligible-link work stealing
 *
 * Each [TxEntry] carries a set of [TxEntry.eligibleLinks] rather than a
 * single target. The first link whose TX coroutine becomes free and calls
 * [pollFor] claims the entry exclusively, enabling natural load-balancing
 * across redundant paths without any explicit scheduler.
 *
 * ## Handles
 *
 * [enqueue] returns a [TxHandle] that allows the caller to receive a
 * notification when the entry is transmitted ([TxHandle.onSent]), cancel the
 * entry ([TxHandle.cancel]), replace its frame bytes ([TxHandle.replace]), or
 * swap its queue position with another entry ([TxHandle.swap]).
 *
 * ## Suspension
 *
 * [pollFor] suspends the calling coroutine until an eligible frame is
 * available. When [enqueue] finds a waiting coroutine for any of the entry's
 * eligible links it completes the deferred directly, bypassing the sorted set.
 *
 * ## Thread safety
 *
 * All mutations to [entries], [entryIndex], and [handleIndex] are guarded by [mutex].
 */
class TxQueue(rules: List<Rule> = defaultRules()) {
    private val mutex = Mutex()

    /** Composite comparator built by chaining [rules] in order. */
    private val comparator: Rule = rules.reduce { acc, rule -> acc.then(rule) }

    /** Sorted set of pending entries, ordered by the composite [comparator]. */
    private val entries = TreeSet<TxEntry>(comparator)

    /** Maps handle [TxHandle.id] to the current [TxEntry] in [entries]. Updated by [replace] and [swap]. */
    private val entryIndex = HashMap<Long, TxEntry>()

    /** Maps handle [TxHandle.id] to its [TxHandle], so [pollFor] can complete [TxHandle.sentDeferred]. */
    private val handleIndex = HashMap<Long, TxHandle>()

    /**
     * Per-link queues of suspended [pollFor] callers waiting for a frame.
     * When a frame is enqueued whose eligible set includes a link that has a
     * waiting coroutine, the deferred is resolved immediately without touching
     * [entries].
     */
    private val waiters = ConcurrentHashMap<String, ArrayDeque<CompletableDeferred<TxEntry>>>()

    /**
     * Adds [entry] to the queue and returns a [TxHandle] for tracking and
     * mutating it. If a link TX loop is already waiting for an eligible frame,
     * the entry is delivered immediately (bypassing the sorted set) and
     * [TxHandle.isSent] will already be `true` when this call returns.
     *
     * Eligible links are checked in iteration order; the first link with a
     * waiting coroutine claims the entry. If no waiter is found the entry is
     * parked in the sorted set until a link calls [pollFor].
     */
    suspend fun enqueue(entry: TxEntry): TxHandle {
        val handle = TxHandle(entry.insertionOrder, this)
        mutex.withLock {
            val waiter = entry.eligibleLinks
                .firstNotNullOfOrNull { link -> waiters[link.id]?.takeIf { it.isNotEmpty() } }
                ?.removeFirst()
            if (waiter != null) {
                waiter.complete(entry)
                handle.sentDeferred.complete(Unit)
            } else {
                entries.add(entry)
                entryIndex[handle.id] = entry
                handleIndex[handle.id] = handle
            }
        }
        return handle
    }

    /**
     * Returns the highest-priority pending [TxEntry] for which [link] is
     * eligible, suspending the caller until one is available.
     *
     * The deferred is registered under the mutex to eliminate the TOCTOU race
     * between "nothing available" and "enqueue arrives": if a frame is enqueued
     * between the check and the suspension, the enqueue path will find the waiter
     * and complete the deferred before [await] is even called.
     *
     * When an entry is claimed, [TxHandle.sentDeferred] is completed so that
     * any [TxHandle.onSent] callbacks fire.
     */
    suspend fun pollFor(link: Link): TxEntry {
        val deferred = CompletableDeferred<TxEntry>()
        mutex.withLock {
            val entry = entries.firstOrNull { link in it.eligibleLinks }
            if (entry != null) {
                entries.remove(entry)
                entryIndex.remove(entry.insertionOrder)
                val handle = handleIndex.remove(entry.insertionOrder)
                deferred.complete(entry)
                handle?.sentDeferred?.complete(Unit)
            } else {
                waiters.getOrPut(link.id) { ArrayDeque() }.addLast(deferred)
            }
        }
        return deferred.await()
    }

    /**
     * Removes the entry referenced by [handle] from the queue.
     * Returns `false` if the entry was already transmitted.
     */
    internal suspend fun cancel(handle: TxHandle): Boolean = mutex.withLock {
        val entry = entryIndex.remove(handle.id) ?: return@withLock false
        handleIndex.remove(handle.id)
        entries.remove(entry)
        true
    }

    /**
     * Swaps the frame bytes of the entry referenced by [handle] while keeping
     * it at its current position in the queue.
     * Returns `false` if the entry was already transmitted.
     */
    internal suspend fun replace(handle: TxHandle, newFrame: ByteArray): Boolean = mutex.withLock {
        val entry = entryIndex[handle.id] ?: return@withLock false
        entries.remove(entry)
        val updated = entry.copy(frame = newFrame)
        entries.add(updated)
        entryIndex[handle.id] = updated
        true
    }

    /**
     * Swaps the queue positions of the entries referenced by [h1] and [h2] by
     * exchanging their [TxEntry.userPriority] and [TxEntry.enqueuedAt] ordering
     * keys. Returns `false` if either entry was already transmitted.
     */
    internal suspend fun swap(h1: TxHandle, h2: TxHandle): Boolean = mutex.withLock {
        val e1 = entryIndex[h1.id] ?: return@withLock false
        val e2 = entryIndex[h2.id] ?: return@withLock false
        entries.remove(e1)
        entries.remove(e2)
        val e1new = e1.copy(userPriority = e2.userPriority, enqueuedAt = e2.enqueuedAt)
        val e2new = e2.copy(userPriority = e1.userPriority, enqueuedAt = e1.enqueuedAt)
        entries.add(e1new)
        entries.add(e2new)
        entryIndex[h1.id] = e1new
        entryIndex[h2.id] = e2new
        true
    }
}
