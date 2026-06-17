package batman

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
        ttl: UByte = 10u
    ) = Frame.OgmFrame(Ogm(origId, sendId, seq, ttl))

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
        gid: GroupId = GroupId(10u, 1u),
        activeRoot: UShort = 10u
    ) = Frame.BeaconFrame(nextHop, src, gid, activeRoot)

    private fun multicast(
        src: UShort = 2u,
        gid: GroupId = GroupId(10u, 1u),
        seq: UShort = 42u,
        ttl: UByte = 5u,
        payload: ByteArray = byteArrayOf(0xAA.toByte(), 0xBB.toByte())
    ) = Frame.MulticastFrame(src, gid, seq, ttl, payload)

    private fun invite(
        nextHop: UShort = 4u,
        src: UShort = 10u,
        dst: UShort = 7u,
        gid: GroupId = GroupId(10u, 2u),
        deputies: List<UShort> = emptyList()
    ) = Frame.InviteFrame(nextHop, src, dst, gid, deputies)

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

        @Test fun `round-trip max payload size (255 bytes)`() {
            val payload = ByteArray(255) { it.toByte() }
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

        @Test fun `encoded length is 7 + payload bytes`() {
            val payload = ByteArray(10)
            assertEquals(17, encode(data(payload = payload)).size)
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
            // Lie about payload length: set payloadLen byte to 100
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
            val f = beacon(nextHop = 0xFFFu, src = 0xFFFu, gid = GroupId(0xFFFu, 0xFFFFu), activeRoot = 0xFFFu)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip zero values`() {
            val f = beacon(nextHop = 0u, src = 0u, gid = GroupId(0u, 0u), activeRoot = 0u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip activeRoot different from group owner`() {
            // Deputy scenario: group owned by node 10 but activeRoot is deputy 99
            val f = beacon(gid = GroupId(10u, 0u), activeRoot = 99u)
            assertEquals(f, roundTrip(f))
        }

        @Test fun `encoded length is 9 bytes`() {
            assertEquals(9, encode(beacon()).size)
        }

        @Test fun `type nibble is TYPE_BEACON (2)`() {
            assertEquals(2, (encode(beacon())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            assertNull(decode(encode(beacon()).copyOf(8)))
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
                gid = GroupId(0xFFFu, 0xFFFFu),
                seq = 0xFFFFu,
                ttl = 15u,
                payload = ByteArray(255) { 0xFF.toByte() }
            )
            val decoded = roundTrip(f) as Frame.MulticastFrame
            assertEquals(f.srcId,   decoded.srcId)
            assertEquals(f.groupId, decoded.groupId)
            assertEquals(f.seqNum,  decoded.seqNum)
            assertEquals(f.ttl,     decoded.ttl)
            assertArrayEquals(f.payload, decoded.payload)
        }

        @Test fun `encoded length is 9 + payload bytes`() {
            assertEquals(11, encode(multicast(payload = ByteArray(2))).size)
        }

        @Test fun `type nibble is TYPE_MULTICAST (3)`() {
            assertEquals(3, (encode(multicast())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            assertNull(decode(encode(multicast()).copyOf(8)))
        }
    }

    // ── InviteFrame ───────────────────────────────────────────────────────────

    @Nested inner class InviteFrameTests {

        @Test fun `round-trip no deputies`() {
            assertEquals(invite(), roundTrip(invite()))
        }

        @Test fun `round-trip one deputy`() {
            val f = invite(deputies = listOf(50u))
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip three deputies`() {
            val f = invite(deputies = listOf(50u, 100u, 200u))
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip boundary values with deputies`() {
            val f = invite(
                nextHop  = 0xFFFu,
                src      = 0xFFFu,
                dst      = 0xFFFu,
                gid      = GroupId(0xFFFu, 0xFFFFu),
                deputies = listOf(0xFFFu, 0u)
            )
            assertEquals(f, roundTrip(f))
        }

        @Test fun `round-trip zero values no deputies`() {
            val f = invite(nextHop = 0u, src = 0u, dst = 0u, gid = GroupId(0u, 0u))
            assertEquals(f, roundTrip(f))
        }

        @Test fun `encoded length is 9 bytes with no deputies`() {
            assertEquals(9, encode(invite()).size)
        }

        @Test fun `encoded length grows by 2 bytes per deputy`() {
            assertEquals(11, encode(invite(deputies = listOf(1u))).size)
            assertEquals(13, encode(invite(deputies = listOf(1u, 2u))).size)
            assertEquals(15, encode(invite(deputies = listOf(1u, 2u, 3u))).size)
        }

        @Test fun `type nibble is TYPE_INVITE (4)`() {
            assertEquals(4, (encode(invite())[0].toInt() and 0xFF) ushr 4)
        }

        @Test fun `truncated frame decodes to null`() {
            assertNull(decode(encode(invite()).copyOf(8)))
        }

        @Test fun `frame truncated mid-deputy list decodes to null`() {
            val raw = encode(invite(deputies = listOf(50u, 100u)))
            // Truncate after the deputy count byte but before all deputy bytes
            assertNull(decode(raw.copyOf(10)))
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
            val nibbles = listOf(ogm(), data(), beacon(), multicast(), invite())
                .map { (encode(it)[0].toInt() and 0xFF) ushr 4 }
            assertEquals(nibbles.distinct().size, nibbles.size)
        }
    }
}
