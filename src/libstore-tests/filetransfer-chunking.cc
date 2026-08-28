#include <cstdint>

#include <gtest/gtest.h>

#include "nix/store/filetransfer-impl.hh"

namespace nix {

// ---------------------------------------------------------------------------
// ChunkPlan — chunk count and byte ranges
// ---------------------------------------------------------------------------

TEST(ChunkPlan, exactMultiple)
{
    ChunkPlan plan{.fileSize = 128, .chunkSize = 32};
    EXPECT_EQ(plan.numChunks(), 4u);
    EXPECT_EQ(plan.range(0).first, 0u);
    EXPECT_EQ(plan.range(0).last, 31u);
    EXPECT_EQ(plan.range(3).first, 96u);
    EXPECT_EQ(plan.range(3).last, 127u);
}

TEST(ChunkPlan, remainderLastChunkIsShort)
{
    ChunkPlan plan{.fileSize = 100, .chunkSize = 32};
    EXPECT_EQ(plan.numChunks(), 4u);
    EXPECT_EQ(plan.range(3).first, 96u);
    EXPECT_EQ(plan.range(3).last, 99u);
}

TEST(ChunkPlan, singleChunkWhenFileFits)
{
    ChunkPlan plan{.fileSize = 32, .chunkSize = 32};
    EXPECT_EQ(plan.numChunks(), 1u);
    EXPECT_EQ(plan.range(0).first, 0u);
    EXPECT_EQ(plan.range(0).last, 31u);
}

TEST(ChunkPlan, oneByteOverChunkSize)
{
    ChunkPlan plan{.fileSize = 33, .chunkSize = 32};
    EXPECT_EQ(plan.numChunks(), 2u);
    EXPECT_EQ(plan.range(1).first, 32u);
    EXPECT_EQ(plan.range(1).last, 32u);
}

TEST(ChunkPlan, largeFileNoOverflow)
{
    // 40 GiB file, 64 MiB chunks: indices and offsets must not truncate.
    const uint64_t gib = 1024ull * 1024 * 1024;
    ChunkPlan plan{.fileSize = 40 * gib, .chunkSize = 64ull * 1024 * 1024};
    EXPECT_EQ(plan.numChunks(), 640u);
    EXPECT_EQ(plan.range(639).last, 40 * gib - 1);
}

// ---------------------------------------------------------------------------
// chunkDispatchCount — adaptive window top-up
// ---------------------------------------------------------------------------

struct DispatchCase
{
    std::string description;
    uint64_t remainingChunks;
    size_t inFlight;
    size_t activeHandles; // pool-wide, includes inFlight
    size_t httpConnections;
    size_t expected;
};

class ChunkDispatchCountTest : public ::testing::TestWithParam<DispatchCase>
{};

TEST_P(ChunkDispatchCountTest, yieldsExpectedCount)
{
    auto & p = GetParam();
    EXPECT_EQ(chunkDispatchCount(p.remainingChunks, p.inFlight, p.activeHandles, p.httpConnections), p.expected);
}

INSTANTIATE_TEST_SUITE_P(
    chunkDispatchCount,
    ChunkDispatchCountTest,
    ::testing::Values(
        // Idle pool: we are the only transfer (1 in flight), 16 slots total
        // → the whole pool belongs to us → dispatch 15 more.
        DispatchCase{"idle_pool_fans_out", 100, 1, 1, 16, 15},
        // Busy pool: 15 other transfers active → window stays 1.
        DispatchCase{"busy_pool_stays_sequential", 100, 1, 16, 16, 0},
        // Pool oversubscribed by others (curl queueing) → no underflow.
        DispatchCase{"oversubscribed_pool", 100, 1, 21, 16, 0},
        // Half-busy pool: 8 others → our window is the free half.
        DispatchCase{"half_busy_pool", 100, 1, 9, 16, 7},
        // Fewer chunks remain than idle slots → dispatch only what's left.
        DispatchCase{"remaining_caps_dispatch", 3, 1, 1, 16, 3},
        // Nothing left to dispatch.
        DispatchCase{"no_remaining", 0, 4, 4, 16, 0},
        // Sole transfer at full window: own chunks must NOT count as pool
        // pressure — the window is sustained at http-connections, not
        // decayed toward half of it.
        DispatchCase{"full_window_is_sustained", 100, 8, 8, 8, 0},
        DispatchCase{"own_completion_is_backfilled", 100, 7, 7, 8, 1},
        // Window above target (pool got busier) → no dispatch, no underflow.
        DispatchCase{"window_above_target", 100, 8, 20, 16, 0},
        // Unlimited pool (http-connections = 0) → bounded by remaining.
        DispatchCase{"unlimited_pool", 5, 1, 1, 0, 5},
        // Nothing in flight yet (probe just finished) → dispatch at least 1
        // even when the pool is saturated by others.
        DispatchCase{"saturated_pool_still_progresses", 100, 0, 16, 16, 1}),
    [](const auto & info) { return info.param.description; });

TEST(ChunkPlan, hugeFileSizeDoesNotOverflow)
{
    /* A malicious narinfo can claim FileSize near 2^64; the chunk count
       must not wrap to 0 (which would hang the drain loop). */
    ChunkPlan plan{.fileSize = std::numeric_limits<uint64_t>::max(), .chunkSize = 1024 * 1024};
    EXPECT_EQ(plan.numChunks(), std::numeric_limits<uint64_t>::max() / (1024 * 1024) + 1);
}

} // namespace nix
