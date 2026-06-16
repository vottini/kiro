package batman

import java.util.BitSet
import java.util.concurrent.ConcurrentHashMap

class SeenWindowCache(private val windowSize: Int = 64) {

    private class Window(val bits: BitSet = BitSet(64)) {
        var latestSeq: UShort = 0u
        var initialized: Boolean = false
    }

    private val windows = ConcurrentHashMap<NodeId, Window>()

    fun markIfNew(originator: NodeId, seq: UShort): Boolean {
        val window = windows.getOrPut(originator) { Window() }
        synchronized(window) {
            if (!window.initialized) {
                window.latestSeq = seq
                window.initialized = true
                window.bits.set(0)
                return true
            }

            val diff = seqDiff(seq, window.latestSeq)

            return when {
                diff > 0 -> {
                    // seq is newer — advance the window
                    if (diff >= windowSize) window.bits.clear() else shiftRight(window.bits, diff)
                    window.latestSeq = seq
                    window.bits.set(0)
                    true
                }
                diff == 0 -> false  // latestSeq itself, already seen
                else -> {
                    // seq is older — check if it falls within the window
                    val offset = -diff
                    if (offset >= windowSize) return false   // too old, reject
                    if (window.bits[offset]) return false    // already seen
                    window.bits.set(offset)
                    true
                }
            }
        }
    }

    // Positive = seq is newer than reference; negative = older. Handles UShort wraparound.
    private fun seqDiff(seq: UShort, reference: UShort): Int {
        val diff = (seq.toInt() - reference.toInt() + 65536) % 65536
        return if (diff < 32768) diff else diff - 65536
    }

    // Shift all bits toward higher indices (making room at index 0 for the new latest seq).
    // bit[i] = seen(latestSeq - i), so shifting right by n means latestSeq advanced by n.
    private fun shiftRight(bits: BitSet, n: Int) {
        for (i in windowSize - 1 downTo 0) {
            bits[i] = if (i >= n) bits[i - n] else false
        }
    }
}
