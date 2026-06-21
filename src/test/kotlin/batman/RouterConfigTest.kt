package batman

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RouterConfigTest {

    // ── ogmInterval formula ───────────────────────────────────────────────────

    @Test fun `50 bps 40 nodes yields ~77 s interval and purge mult 3`() {
        val cfg = recommendedConfig(50L, expectedNodes = 40)
        // N×48 / (0.5×50) = 40×48/25 = 76.8 s
        assertTrue(cfg.ogmInterval in 76.seconds..78.seconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(3, cfg.neighborPurgeMultiplier)
    }

    @Test fun `500 bps 40 nodes yields ~7·7 s interval and purge mult 5`() {
        val cfg = recommendedConfig(500L, expectedNodes = 40)
        // 40×48 / (0.5×500) = 7.68 s → 60/7.68 ≈ 7.8 → rounds to 8 → clamped to 5
        assertTrue(cfg.ogmInterval in 7.seconds..8.seconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    @Test fun `5 kbps 40 nodes yields ~770 ms interval and purge mult 5`() {
        val cfg = recommendedConfig(5_000L, expectedNodes = 40)
        // 40×48 / (0.5×5000) = 0.768 s
        assertTrue(cfg.ogmInterval in 760.milliseconds..780.milliseconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    @Test fun `50 kbps 40 nodes yields ~77 ms interval and purge mult 5`() {
        val cfg = recommendedConfig(50_000L, expectedNodes = 40)
        // 40×48 / (0.5×50000) = 0.0768 s
        assertTrue(cfg.ogmInterval in 76.milliseconds..78.milliseconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    // ── expectedNodes scales the interval linearly ────────────────────────────

    @Test fun `interval scales linearly with node count`() {
        val cfg10 = recommendedConfig(1_000L, expectedNodes = 10)
        val cfg20 = recommendedConfig(1_000L, expectedNodes = 20)
        // doubling nodes doubles the interval
        val ratio = cfg20.ogmInterval.inWholeMilliseconds.toDouble() /
                    cfg10.ogmInterval.inWholeMilliseconds.toDouble()
        assertTrue(ratio in 1.9..2.1, "ratio=$ratio")
    }

    // ── dataFraction shifts the budget ────────────────────────────────────────

    @Test fun `tighter data fraction produces longer interval`() {
        val cfg50 = recommendedConfig(1_000L, expectedNodes = 10, dataFraction = 0.5)
        val cfg80 = recommendedConfig(1_000L, expectedNodes = 10, dataFraction = 0.8)
        assertTrue(cfg80.ogmInterval > cfg50.ogmInterval,
            "80% data fraction should require longer interval than 50%")
    }

    // ── floor at 50 ms for very fast links ────────────────────────────────────

    @Test fun `very high bandwidth is floored at 50 ms`() {
        val cfg = recommendedConfig(1_000_000_000L, expectedNodes = 10)
        assertEquals(50.milliseconds, cfg.ogmInterval)
    }

    // ── purgeTimeout is derived correctly ────────────────────────────────────

    @Test fun `purgeTimeout equals ogmInterval times purge multiplier`() {
        val cfg = recommendedConfig(500L, expectedNodes = 40)
        assertEquals(cfg.ogmInterval * cfg.neighborPurgeMultiplier, cfg.purgeTimeout)
    }

    // ── purge mult transitions ────────────────────────────────────────────────

    @Test fun `slow link purge mult is 3`() {
        // Any interval > 20 s rounds to purge ≤ 3
        val cfg = recommendedConfig(50L, expectedNodes = 40)
        assertEquals(3, cfg.neighborPurgeMultiplier)
    }

    @Test fun `fast link purge mult is 5`() {
        // Any interval < 12 s rounds to purge ≥ 5
        val cfg = recommendedConfig(50_000L, expectedNodes = 40)
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    // ── validation ────────────────────────────────────────────────────────────

    @Test fun `invalid bandwidth throws`() {
        var threw = false
        try { recommendedConfig(0L) } catch (e: IllegalArgumentException) { threw = true }
        assertTrue(threw)
    }

    @Test fun `invalid node count throws`() {
        var threw = false
        try { recommendedConfig(1000L, expectedNodes = 0) } catch (e: IllegalArgumentException) { threw = true }
        assertTrue(threw)
    }

    @Test fun `invalid data fraction throws`() {
        var threw = false
        try { recommendedConfig(1000L, dataFraction = 1.5) } catch (e: IllegalArgumentException) { threw = true }
        assertTrue(threw)
    }
}
