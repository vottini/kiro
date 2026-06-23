package systems.untangle.kiro.demo

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import systems.untangle.kiro.Link
import java.io.*
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A [Link] that models one shared broadcast medium using a directory of named pipes.
 *
 * Each node on the medium owns one input pipe: `<mediumDir>/node_<selfId>`.
 * Broadcasting writes the frame to every *other* `node_*` pipe found in the
 * directory at the time of the call, so new nodes are discovered automatically
 * on the next broadcast without any reconfiguration.
 *
 * Frame framing: 2-byte big-endian unsigned length prefix followed by raw bytes.
 */
class PipeLink(
    override val id: String,
    private val mediumDir: Path,
    private val selfId: UShort,
    override val ogmInterval: Duration = 5.seconds,
) : Link {

    private val selfPipe = mediumDir.resolve("node_$selfId")

    /** Set to false to simulate the node going offline (drops all tx and rx). */
    @Volatile var online: Boolean = true

    private val _frames = MutableSharedFlow<ByteArray>(extraBufferCapacity = 512)
    override val frames: Flow<ByteArray> = _frames

    /** Creates the medium directory and this node's input pipe if they don't exist yet. */
    fun initialize() {
        Files.createDirectories(mediumDir)
        if (!Files.exists(selfPipe)) {
            ProcessBuilder("mkfifo", selfPipe.toString()).start().also { proc ->
                proc.inputStream.close()
                proc.outputStream.close()
                proc.errorStream.close()
                proc.waitFor()
            }
        }
    }

    /**
     * Starts a background coroutine that reads frames from this node's pipe.
     * Reopens automatically after each EOF so the reader is always ready for
     * the next broadcaster's open-write-close cycle.
     */
    fun startReading(scope: CoroutineScope) {
        scope.launch(Dispatchers.IO) {
            while (isActive) {
                try {
                    FileInputStream(selfPipe.toFile()).use { fis ->
                        val din = DataInputStream(BufferedInputStream(fis))
                        while (true) {
                            val len = din.readUnsignedShort()
                            val bytes = ByteArray(len)
                            din.readFully(bytes)
                            if (online) _frames.tryEmit(bytes)
                        }
                    }
                } catch (_: EOFException) {
                    // Writer closed its end; loop back to reopen and wait for next write.
                } catch (_: IOException) {
                    if (isActive) delay(50)
                }
            }
        }
    }

    /** All peer pipes currently present in the medium directory, excluding our own. */
    private fun peerPipes(): List<Path> = try {
        Files.list(mediumDir).use { stream ->
            stream.filter { it != selfPipe && it.fileName.toString().startsWith("node_") }
                  .toList()
        }
    } catch (_: IOException) { emptyList() }

    override suspend fun broadcast(frame: ByteArray) {
        if (!online) return
        // Build the framed packet (2-byte big-endian length prefix + payload) before
        // entering the IO context so it is allocated once for all peers.
        val packet = ByteArray(2 + frame.size)
        packet[0] = (frame.size ushr 8).toByte()
        packet[1] = (frame.size and 0xFF).toByte()
        frame.copyInto(packet, 2)
        withContext(Dispatchers.IO) {
            for (pipe in peerPipes()) {
                try {
                    FileOutputStream(pipe.toFile()).use { fos ->
                        // Single write call: on Linux, writes ≤ PIPE_BUF (4096) bytes to a
                        // FIFO are atomic even across concurrent writers, so our length prefix
                        // and payload cannot be interleaved with another writer's bytes.
                        fos.write(packet)
                        fos.flush()
                    }
                } catch (_: IOException) {
                    // Peer not yet listening; skip.
                }
            }
        }
    }
}
