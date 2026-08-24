package systems.untangle.kiro

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A [Link] that models one shared broadcast medium using UDP IP multicast.
 *
 * All nodes that share the same [multicastGroup] address and [port] form a
 * single broadcast domain — any frame sent by one node is received by all
 * others on that medium, including the sender (IP multicast loopback). The
 * router's built-in `originatorId == selfId` guard and deduplication window
 * prevent own OGMs from being processed twice.
 *
 * Multiple instances with different ports or multicast addresses model
 * separate broadcast media. A node connected to two such instances bridges
 * them exactly as it would two radio interfaces.
 *
 * Nodes on different machines on the same LAN can participate in the same
 * medium as long as multicast routing is enabled on the network.
 *
 * ## Framing
 *
 * UDP preserves datagram boundaries, so no length-prefix framing is needed
 * and there is no risk of byte-level interleaving between concurrent writers
 * (unlike stream-based transports). Each datagram carries exactly one frame.
 * The practical per-frame ceiling is ~65 kB (IP datagram limit minus headers);
 * the codec imposes no additional restriction.
 *
 * ## Lifecycle
 *
 * Pass this link to [KiroRouter.start] or [KiroRouter.addLink]; the router calls
 * [start] and [stop] automatically. No manual lifecycle management is required.
 *
 * @param id Human-readable medium name used as a routing table key
 *   (e.g. `"udp:239.0.0.1:5001"`). Must be unique among all links on a node.
 * @param multicastGroup IPv4 multicast address. Use an address in the
 *   administratively scoped 239.0.0.0/8 range for LAN-local deployments.
 * @param port UDP port. All nodes on the same medium must use the same port.
 * @param networkInterface Network interface to join the multicast group on. When
 *   non-null, the OS restricts group membership to that interface so datagrams
 *   arriving on other interfaces — even for the same multicast group and port —
 *   are not delivered to this socket. This allows two `UdpMulticastLink` instances
 *   with the same group/port to coexist on different physical interfaces without
 *   cross-contamination. When `null`, the OS picks the default interface.
 * @param ogmInterval How often this node emits OGMs on this link. Longer
 *   intervals reduce bandwidth but slow route convergence.
 * @param outboundTransform Applied to every frame before it is transmitted.
 *   Return `null` to suppress the frame (e.g. serialization error). Typical
 *   use: encryption, HMAC tagging.
 * @param inboundTransform Applied to every received datagram before it is
 *   emitted to [frames]. Return `null` to drop the frame (e.g. failed MAC
 *   verification). Typical use: decryption, HMAC validation.
 */
class UdpMulticastLink(
    override val id: String,
    val multicastGroup: String = "239.0.0.1",
    val port: Int,
    val networkInterface: NetworkInterface? = null,
    override val ogmInterval: Duration = 5.seconds,
    override val bandwidthBps: Long = 100_000_000L,
    private val outboundTransform: (suspend (ByteArray) -> ByteArray?)? = null,
    private val inboundTransform:  (suspend (ByteArray) -> ByteArray?)? = null,
) : Link {

    private val group   = InetAddress.getByName(multicastGroup)
    private val groupSa = InetSocketAddress(group, port)

    private val _frames = MutableSharedFlow<ByteArray>(extraBufferCapacity = 512)
    override val frames: Flow<ByteArray> = _frames

    // Always bind to the wildcard address so the kernel delivers multicast packets
    // (whose destination is the group address, not any unicast address) to this socket.
    // Interface isolation is provided entirely by joinGroup: joining on a specific
    // networkInterface restricts membership to that interface, so packets received on
    // other interfaces for the same group/port are not delivered here.
    private val socket = MulticastSocket(null).also { s ->
        s.reuseAddress = true
        s.bind(InetSocketAddress(port))
        s.joinGroup(groupSa, networkInterface)
        // joinGroup only controls inbound delivery; setNetworkInterface controls
        // which interface outbound multicast packets are sent from.
        if (networkInterface != null) s.networkInterface = networkInterface
    }

    /**
     * Starts a background coroutine on [Dispatchers.IO] that receives UDP
     * datagrams from the multicast group and emits each frame to [frames].
     *
     * The coroutine stops when [stop] is called or when [scope] is cancelled,
     * whichever comes first.
     */
    override fun start(scope: CoroutineScope) {
        scope.launch(Dispatchers.IO) {
            val buffer = ByteArray(65536)
            while (isActive) {
                try {
                    val packet = DatagramPacket(buffer, buffer.size)
                    socket.receive(packet)
                    val raw = packet.data.copyOf(packet.length)
                    val frame = if (inboundTransform != null) inboundTransform.invoke(raw) else raw
                    if (frame != null) _frames.tryEmit(frame)
                } catch (_: IOException) {
                    if (isActive) delay(50)
                }
            }
        }
    }

    override suspend fun broadcast(frame: ByteArray) {
        val wire = if (outboundTransform != null) outboundTransform.invoke(frame) ?: return else frame
        withContext(Dispatchers.IO) {
            try {
                socket.send(DatagramPacket(wire, wire.size, group, port))
            } catch (_: IOException) { }
        }
    }

    /**
     * Leaves the multicast group and closes the socket.
     *
     * The scope passed to [start] is cancelled by [KiroRouter] before calling this,
     * which sets [isActive] to false in the receive loop. Closing the socket then
     * unblocks any in-progress [MulticastSocket.receive] so the coroutine exits cleanly.
     */
    override fun stop() {
        runCatching { socket.leaveGroup(groupSa, networkInterface) }
        runCatching { socket.close() }
    }
}
