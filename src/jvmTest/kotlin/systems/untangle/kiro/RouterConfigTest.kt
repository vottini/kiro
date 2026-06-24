package systems.untangle.kiro

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RouterConfigTest {

    // ── ogmInterval formula ───────────────────────────────────────────────────

    @Test fun `50 bps 40 nodes yields ~90 s interval and purge mult 3`() {
        val cfg = recommendedConfig(50L, expectedNodes = 40)
        // N×56 / (0.5×50) = 40×56/25 = 89.6 s
        assertTrue(cfg.ogmInterval in 89.seconds..91.seconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(3, cfg.neighborPurgeMultiplier)
    }

    @Test fun `500 bps 40 nodes yields ~9 s interval and purge mult 5`() {
        val cfg = recommendedConfig(500L, expectedNodes = 40)
        // 40×56 / (0.5×500) = 8.96 s → 60/8.96 ≈ 6.7 → rounds to 7 → clamped to 5
        assertTrue(cfg.ogmInterval in 8.seconds..10.seconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    @Test fun `5 kbps 40 nodes yields ~896 ms interval and purge mult 5`() {
        // Formula gives 0.896 s; pass explicit floor below that to observe the raw result.
        val cfg = recommendedConfig(5_000L, expectedNodes = 40, minOgmInterval = 100.milliseconds)
        // 40×56 / (0.5×5000) = 0.896 s
        assertTrue(cfg.ogmInterval in 890.milliseconds..910.milliseconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    @Test fun `50 kbps 40 nodes yields ~90 ms interval and purge mult 5`() {
        // Formula gives 0.0896 s; pass explicit floor below that to observe the raw result.
        val cfg = recommendedConfig(50_000L, expectedNodes = 40, minOgmInterval = 10.milliseconds)
        // 40×56 / (0.5×50000) = 0.0896 s
        assertTrue(cfg.ogmInterval in 89.milliseconds..91.milliseconds, "ogmInterval=${cfg.ogmInterval}")
        assertEquals(5, cfg.neighborPurgeMultiplier)
    }

    // ── expectedNodes scales the interval linearly ────────────────────────────

    @Test fun `interval scales linearly with node count`() {
        // Use a bandwidth where formula results (0.48 s and 0.96 s) stay below the 5 s floor,
        // so pass a smaller explicit floor to observe the unclipped scaling.
        val cfg10 = recommendedConfig(1_000L, expectedNodes = 10, minOgmInterval = 100.milliseconds)
        val cfg20 = recommendedConfig(1_000L, expectedNodes = 20, minOgmInterval = 100.milliseconds)
        // doubling nodes doubles the interval
        val ratio = cfg20.ogmInterval.inWholeMilliseconds.toDouble() /
                    cfg10.ogmInterval.inWholeMilliseconds.toDouble()
        assertTrue(ratio in 1.9..2.1, "ratio=$ratio")
    }

    // ── dataFraction shifts the budget ────────────────────────────────────────

    @Test fun `tighter data fraction produces longer interval`() {
        // Pass a floor below both formula results (0.96 s and 2.4 s) to observe the effect.
        val cfg50 = recommendedConfig(1_000L, expectedNodes = 10, dataFraction = 0.5, minOgmInterval = 100.milliseconds)
        val cfg80 = recommendedConfig(1_000L, expectedNodes = 10, dataFraction = 0.8, minOgmInterval = 100.milliseconds)
        assertTrue(cfg80.ogmInterval > cfg50.ogmInterval,
            "80% data fraction should require longer interval than 50%")
    }

    // ── floor clamps very fast links ─────────────────────────────────────────

    @Test fun `very high bandwidth is floored at default 5 s`() {
        val cfg = recommendedConfig(1_000_000_000L, expectedNodes = 10)
        assertEquals(5.seconds, cfg.ogmInterval)
    }

    @Test fun `custom minOgmInterval is respected`() {
        val cfg = recommendedConfig(1_000_000_000L, expectedNodes = 10, minOgmInterval = 50.milliseconds)
        assertEquals(50.milliseconds, cfg.ogmInterval)
    }

    // ── purgeTimeout is derived correctly ────────────────────────────────────

    @Test fun `purgeTimeout equals ogmInterval times purge multiplier`() {
        val cfg = recommendedConfig(500L, expectedNodes = 40)
        assertEquals(cfg.ogmInterval * cfg.neighborPurgeMultiplier, cfg.purgeTimeout)
    }

    // ── purge mult transitions ────────────────────────────────────────────────

    @Test fun `slow link purge mult is 3`() {
        // 89.6 s interval → 60/89.6 ≈ 0.67 → rounds to 1 → clamped to 3
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
