package batman

import java.util.concurrent.ConcurrentHashMap

class SeenWindowCache(private val windowSize: Int = 64) {
    private val windows = ConcurrentHashMap<NodeId, ArrayDeque<UShort>>()

    fun markIfNew(originator: NodeId, seq: UShort): Boolean {
        val window = windows.getOrPut(originator) { ArrayDeque() }
        synchronized(window) {
            if (seq in window) return false
            window.addLast(seq)
            if (window.size > windowSize) window.removeFirst()
            return true
        }
    }
}
