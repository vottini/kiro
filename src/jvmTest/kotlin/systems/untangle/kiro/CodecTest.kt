package systems.untangle.kiro

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Nested
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CodecTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    private fun roundTrip(frame: Frame): Frame =
        decode(encode(frame)) ?: error("decode returned null for $frame")

    private fun ogm(
        origId: UShort = 1u,
        sendId: UShort = 2u,
        seq: UShort = 100u,
        ttl: UByte = 10u,
        minBandwidthTier: UByte = 26u
    ) = Frame.OgmFrame(Ogm(origId, sendId, seq, ttl, minBandwidthTier))

    private fun data(
        nextHop: UShort = 3u,
        src: UShort = 1u,
        dst: UShort = 5u,
        ttl: UByte = 8u,
        payload: ByteArray = byteArrayOf(0x11, 0x22)
    ) = Frame.DataFrame(nextHop, src, dst, ttl, payload)

    private fun beacon(
        nextHop: UShort = 3u,
        src: UShort = 7u,
        gid: GroupId = GroupId(0xA01u),   // 10 shl 8 or 1
        activeRoot: UShort = 10u
    ) = Frame.BeaconFrame(nextHop, src, gid, activeRoot)

    private fun multicast(
        src: UShort = 2u,
        gid: GroupId = GroupId(0xA01u),   // 10 shl 8 or 1
        seq: UShort = 42u,
        ttl: UByte = 5u,
        payload: ByteArray = byteArrayOf(0xAA.toByte(), 0xBB.toByte())
    ) = Frame.MulticastFrame(src, gid, seq, ttl, payload)

    // ── OGM ──────────────────────────────────────────────────────────────────

    @Nested inner class OgmFrameTests {

        @Test fun `round-trip typical values`() {
            val f = ogm()
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip zero IDs and TTL`() {
            val f = ogm(origId = 0u, sendId = 0u, seq = 0u, ttl = 0u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip max 12-bit NodeIds and 4-bit TTL`() {
            val f = ogm(origId = 0xFFFu, sendId = 0xFFFu, seq = 0xFFFFu, ttl = 15u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `encoded length is 6 bytes`() {
            assertEquals(6, encode(ogm()).size)
        }

        @Test fun `type nibble is TYPE_OGM (0)`() {
            assertEquals(0, (encode(ogm())[0].toInt() and 0xFF) ushr 4)
        }
    }

    // ── DataFrame ─────────────────────────────────────────────────────────────

    @Nested inner class DataFrameTests {

        @Test fun `round-trip typical values`() {
            val f = data()
            val decoded = roundTrip(f) as Frame.DataFrame
            assertEquals(f.nextHop, decoded.nextHop)
            assertEquals(f.srcId,   decoded.srcId)
            assertEquals(f.dstId,   decoded.dstId)
            assertEquals(f.ttl,     decoded.ttl)
            assertArrayEquals(f.payload, decoded.payload)
        }

        @Test fun `round-trip empty payload`() {
            val f = data(payload = byteArrayOf())
            val decoded = roundTrip(f) as Frame.DataFrame
            assertArrayEquals(byteArrayOf(), decoded.payload)
        }

        @Test fun `round-trip large payload beyond 255 bytes`() {
            val payload = ByteArray(1000) { it.toByte() }
            val f = data(payload = payload)
            val decoded = roundTrip(f) as Frame.DataFrame
            assertArrayEquals(payload, decoded.payload)
        }

        @Test fun `round-trip boundary NodeIds and TTL`() {
            val f = data(nextHop = 0xFFFu, src = 0u, dst = 0xFFFu, ttl = 15u)
            val decoded = roundTrip(f) as Frame.DataFrame
            assertEquals(f.nextHop, decoded.nextHop)
            assertEquals(f.srcId,   decoded.srcId)
            assertEquals(f.dstId,   decoded.dstId)
            assertEquals(f.ttl,     decoded.ttl)
        }

        @Test fun `encoded length is 7 + payload bytes for small payload`() {
            // 10-byte payload: 6 header + 1 varint byte + 10 payload = 17
            val payload = ByteArray(10)
            assertEquals(17, encode(data(payload = payload)).size)
        }

        @Test fun `encoded length uses 2-byte varint for payload over 127 bytes`() {
            // 128-byte payload: 6 header + 2 varint bytes + 128 payload = 136
            val payload = ByteArray(128)
            assertEquals(136, encode(data(payload = payload)).size)
        }

        @Test fun `type nibble is TYPE_DATA (1)`() {
            assertEquals(1, (encode(data())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            val raw = encode(data())
            assertNull(decode(raw.copyOf(6)))
        }

        @Test fun `declared payload overrun decodes to null`() {
            val raw = encode(data(payload = ByteArray(5)))
            // Corrupt the varint length byte (byte 6) to claim 100 bytes
            raw[6] = 100
            assertNull(decode(raw))
        }
    }

    // ── BeaconFrame ───────────────────────────────────────────────────────────

    @Nested inner class BeaconFrameTests {

        @Test fun `round-trip typical values`() {
            assertEquals(beacon(), roundTrip(beacon()))
        }

        @Test fun `round-trip max field values`() {
            val f = beacon(nextHop = 0xFFFu, src = 0xFFFu, gid = GroupId(0xFFFFFu), activeRoot = 0xFFFu)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip zero values`() {
            val f = beacon(nextHop = 0u, src = 0u, gid = GroupId(0u), activeRoot = 0u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip activeRoot differs from group id`() {
            val f = beacon(gid = GroupId(0xA00u), activeRoot = 99u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `encoded length is 8 bytes`() {
            assertEquals(8, encode(beacon()).size)
        }

        @Test fun `type nibble is TYPE_BEACON (2)`() {
            assertEquals(2, (encode(beacon())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            assertNull(decode(encode(beacon()).copyOf(7)))
        }
    }

    // ── MulticastFrame ────────────────────────────────────────────────────────

    @Nested inner class MulticastFrameTests {

        @Test fun `round-trip typical values`() {
            val f = multicast()
            val decoded = roundTrip(f) as Frame.MulticastFrame
            assertEquals(f.srcId,   decoded.srcId)
            assertEquals(f.groupId, decoded.groupId)
            assertEquals(f.seqNum,  decoded.seqNum)
            assertEquals(f.ttl,     decoded.ttl)
            assertArrayEquals(f.payload, decoded.payload)
        }

        @Test fun `round-trip empty payload`() {
            val f = multicast(payload = byteArrayOf())
            assertArrayEquals(byteArrayOf(), (roundTrip(f) as Frame.MulticastFrame).payload)
        }

        @Test fun `round-trip boundary values`() {
            val f = multicast(
                src = 0xFFFu,
                gid = GroupId(0xFFFFFu),
                seq = 0xFFFFu,
                ttl = 15u,
                payload = ByteArray(1000) { 0xFF.toByte() }
            )
            val decoded = roundTrip(f) as Frame.MulticastFrame
            assertEquals(f.srcId,   decoded.srcId)
            assertEquals(f.groupId, decoded.groupId)
            assertEquals(f.seqNum,  decoded.seqNum)
            assertEquals(f.ttl,     decoded.ttl)
            assertArrayEquals(f.payload, decoded.payload)
        }

        @Test fun `encoded length is 8 + payload bytes for small payload`() {
            // 2-byte payload: 7 header + 1 varint byte + 2 payload = 10
            assertEquals(10, encode(multicast(payload = ByteArray(2))).size)
        }

        @Test fun `encoded length uses 2-byte varint for payload over 127 bytes`() {
            // 128-byte payload: 7 header + 2 varint bytes + 128 payload = 137
            assertEquals(137, encode(multicast(payload = ByteArray(128))).size)
        }

        @Test fun `type nibble is TYPE_MULTICAST (3)`() {
            assertEquals(3, (encode(multicast())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            assertNull(decode(encode(multicast()).copyOf(7)))
        }
    }

    // ── decode edge cases ─────────────────────────────────────────────────────

    @Nested inner class DecodeEdgeCases {

        @Test fun `empty array decodes to null`() {
            assertNull(decode(byteArrayOf()))
        }

        @Test fun `unknown type nibble decodes to null`() {
            assertNull(decode(byteArrayOf(0x50.toByte()))) // type = 5, reserved
        }

        @Test fun `all frame types survive distinct type nibbles`() {
            // Ensures no two frame types share the same nibble.
            val nibbles = listOf(ogm(), data(), beacon(), multicast())
                .map { (encode(it)[0].toInt() and 0xFF) ushr 4 }
            assertEquals(nibbles.distinct().size, nibbles.size)
        }
    }
}
