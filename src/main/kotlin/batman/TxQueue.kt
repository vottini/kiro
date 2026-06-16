package batman

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap

enum class TxPriority { CONTROL, DATA }

data class TxEntry(
    val frame: ByteArray,
    val targetLink: Link,
    val priority: TxPriority,
    val enqueuedAt: Instant = Instant.now()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TxEntry) return false
        return priority == other.priority &&
            enqueuedAt == other.enqueuedAt &&
            targetLink.id == other.targetLink.id &&
            frame.contentEquals(other.frame)
    }

    override fun hashCode(): Int {
        var result = frame.contentHashCode()
        result = 31 * result + targetLink.id.hashCode()
        result = 31 * result + priority.hashCode()
        result = 31 * result + enqueuedAt.hashCode()
        return result
    }
}

fun defaultComparator(): Comparator<TxEntry> =
    compareBy<TxEntry> { it.priority.ordinal }
        .thenBy { it.enqueuedAt }
        .thenBy { it.hashCode() }   // stable total order required by TreeSet

class TxQueue(comparator: Comparator<TxEntry> = defaultComparator()) {
    private val mutex = Mutex()
    private val entries = TreeSet<TxEntry>(comparator)
    private val waiters = ConcurrentHashMap<String, ArrayDeque<CompletableDeferred<TxEntry>>>()

    suspend fun enqueue(entry: TxEntry) = mutex.withLock {
        val pending = waiters[entry.targetLink.id]
        if (!pending.isNullOrEmpty()) {
            pending.removeFirst().complete(entry)
        } else {
            entries.add(entry)
        }
    }

    suspend fun pollFor(link: Link): TxEntry {
        val deferred = CompletableDeferred<TxEntry>()
        mutex.withLock {
            val entry = entries.firstOrNull { it.targetLink === link }
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
