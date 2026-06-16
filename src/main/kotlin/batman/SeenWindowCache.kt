package batman

import java.util.BitSet
import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks which (originator, sequence-number) pairs have already been processed,
 * used to deduplicate OGMs and multicast frames that arrive via multiple paths.
 *
 * ## Why a bitmask window instead of a set?
 *
 * A naive `HashSet<UShort>` would either grow forever or require an explicit TTL
 * per entry. Instead, this class mirrors the classic BATMAN approach: for each
 * originator we maintain a sliding bitmask window of [windowSize] bits anchored
 * on the highest sequence number seen so far ([Window.latestSeq]).
 *
 *   bit[0] = latestSeq has been seen
 *   bit[1] = latestSeq - 1 has been seen
 *   bit[i] = latestSeq - i has been seen
 *
 * When a newer sequence number arrives the window shifts right (toward higher
 * indices), making room for the new entry at bit[0]. Sequence numbers older than
 * [windowSize] positions behind [Window.latestSeq] are silently rejected.
 *
 * ## UShort wraparound
 *
 * Sequence numbers are UShort (0–65535). After 65535 comes 0. A naïve comparison
 * (new > old) would misidentify 0 as older than 65535. [seqDiff] handles this by
 * interpreting the modular distance: if the raw difference is < 32768 the sequence
 * is newer; if it is ≥ 32768 it is older (the other half of the number space).
 * This correctly handles any single wraparound within a [windowSize]-wide window.
 */
class SeenWindowCache(private val windowSize: Int = 64) {

    /**
     * Per-originator sliding window state.
     *
     * [bits] is the bitmask described above. [latestSeq] is the highest sequence
     * number seen so far. Access must be synchronised on the [Window] instance
     * itself since multiple [Link.frames] collectors can call [markIfNew] concurrently
     * for the same originator.
     */
    private class Window(val bits: BitSet = BitSet(64)) {
        var latestSeq: UShort = 0u
        var initialized: Boolean = false
    }

    /** One [Window] per originator node, created lazily on first contact. */
    private val windows = ConcurrentHashMap<NodeId, Window>()

    /**
     * Records that [seq] was received from [originator] and returns whether this
     * is the first time that (originator, seq) combination has been seen.
     *
     * Returns `true`  → caller should process / relay this frame.
     * Returns `false` → duplicate or stale; caller should drop silently.
     *
     * Thread-safe: synchronized per originator window to avoid races between
     * concurrent [Link] receive loops.
     */
    fun markIfNew(originator: NodeId, seq: UShort): Boolean {
        val window = windows.getOrPut(originator) { Window() }
        synchronized(window) {
            // Bootstrap: accept the very first sequence number unconditionally.
            if (!window.initialized) {
                window.latestSeq = seq
                window.initialized = true
                window.bits.set(0)
                return true
            }

            // Compute the signed distance from latestSeq to seq, respecting wraparound.
            val diff = seqDiff(seq, window.latestSeq)

            return when {
                diff > 0 -> {
                    // seq is ahead of our window — advance latestSeq to seq.
                    if (diff >= windowSize) {
                        // Gap is larger than the window: clear everything (too much missed).
                        window.bits.clear()
                    } else {
                        // Shift existing bits right to make room at index 0 for the new seq.
                        shiftRight(window.bits, diff)
                    }
                    window.latestSeq = seq
                    window.bits.set(0)
                    true
                }

                diff == 0 -> false  // seq == latestSeq: already seen and recorded at bit[0].

                else -> {
                    // seq is behind latestSeq — it arrived late or is a duplicate relay.
                    val offset = -diff
                    if (offset >= windowSize) return false   // too old; outside the window entirely
                    if (window.bits[offset]) return false    // already seen within the window
                    window.bits.set(offset)
                    true
                }
            }
        }
    }

    /**
     * Returns the signed sequence-number distance from [reference] to [seq],
     * correctly handling the UShort modular arithmetic (0–65535 wraparound).
     *
     * Result > 0 means [seq] is newer than [reference].
     * Result < 0 means [seq] is older than [reference].
     * Result = 0 means they are equal.
     *
     * The 32768 threshold splits the 65536-element number space in half:
     * if the raw unsigned difference is in [1, 32767] the sequence moved forward;
     * if it is in [32769, 65535] it wrapped around and is actually behind.
     */
    private fun seqDiff(seq: UShort, reference: UShort): Int {
        val diff = (seq.toInt() - reference.toInt() + 65536) % 65536
        return if (diff < 32768) diff else diff - 65536
    }

    /**
     * Shifts all bits in [bits] toward higher indices by [n] positions.
     *
     * Before the shift: bit[i] = seen(latestSeq - i)
     * After the shift:  bit[i] = seen(newLatestSeq - i) where newLatestSeq advanced by n
     *
     * Iterating from high to low avoids overwriting source bits before they are read.
     * Positions 0..(n-1) are cleared because they represent the newly advanced
     * sequence numbers that have not yet been seen.
     */
    private fun shiftRight(bits: BitSet, n: Int) {
        for (i in windowSize - 1 downTo 0) {
            bits[i] = if (i >= n) bits[i - n] else false
        }
    }
}
