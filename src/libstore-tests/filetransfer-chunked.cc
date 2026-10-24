#ifndef _WIN32

#  include <algorithm>
#  include <random>

#  include <gtest/gtest.h>

#  include "nix/store/filetransfer.hh"
#  include "nix/store/local-store.hh"
#  include "nix/store/tests/http-range-server.hh"
#  include "nix/store/tests/https-store.hh"
#  include "nix/util/file-system.hh"
#  include "nix/util/archive.hh"
#  include "nix/util/serialise.hh"

namespace nix {

using test::HttpRangeServer;
using test::RangeServerBehavior;

namespace {

std::string makeBody(size_t size)
{
    std::string body(size, '\0');
    std::mt19937 rng{42};
    std::generate(body.begin(), body.end(), [&]() { return static_cast<char>(rng()); });
    return body;
}

constexpr uint64_t MiB = 1024 * 1024;

struct ChunkedDownloadFixture
{
    FileTransferSettings settings;

    ChunkedDownloadFixture()
    {
        settings.parallelDownloadChunkSize = MiB;
        settings.httpConnections = 4;
        settings.retryDelayMs = 1;
        settings.retryJitter = false;
    }

    std::string fetch(HttpRangeServer & server, std::string_view path, std::optional<uint64_t> expectedSize)
    {
        auto ft = makeFileTransfer(settings);
        FileTransferRequest request(VerbatimURL{server.url(path)});
        request.chunked = ChunkedDownloadHint{.expectedSize = expectedSize};
        StringSink sink;
        ft->download(std::move(request), sink);
        return std::move(sink.s);
    }
};

size_t countRanged(const std::vector<test::RangeServerRequest> & reqs)
{
    size_t n = 0;
    for (auto & r : reqs)
        if (r.rangeHeader)
            n++;
    return n;
}

} // namespace

TEST(ChunkedDownload, fansOutAndReassembles)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(5 * MiB + 12345);
    HttpRangeServer server{{{"/big", body}}, {.etag = "\"v1\""}};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    /* ceil(5 MiB + 12345 / 1 MiB) = 6 ranged requests. */
    EXPECT_EQ(reqs.size(), 6u);
    EXPECT_EQ(countRanged(reqs), 6u);
    /* Every chunk after the probe pins the object with If-Match. */
    for (auto & r : reqs) {
        if (r.rangeHeader && *r.rangeHeader != "bytes=0-1048575")
            EXPECT_EQ(r.ifMatchHeader, std::optional<std::string>("\"v1\""));
    }
}

TEST(ChunkedDownload, fallsBackWhenServerIgnoresRanges)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}, {.supportRanges = false, .advertiseRanges = false}};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    /* One request total: the probe's 200 response is consumed as the whole
       body; no chunks are dispatched. */
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ChunkedDownload, smallFileIsNotChunked)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(MiB / 2);
    HttpRangeServer server{{{"/small", body}}};

    auto fetched = fx.fetch(server, "/small", body.size());

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 1u);
    EXPECT_EQ(countRanged(reqs), 0u);
}

TEST(ChunkedDownload, disabledSettingIsNotChunked)
{
    ChunkedDownloadFixture fx;
    fx.settings.parallelDownloadChunkSize = 0;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 1u);
    EXPECT_EQ(countRanged(reqs), 0u);
}

TEST(ChunkedDownload, unknownSizeIsPlannedFromProbe)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB + 777);
    HttpRangeServer server{{{"/big", body}}, {.etag = "\"v1\""}};

    /* No size hint (e.g. a .narinfo without FileSize): chunk 0 doubles as
       a size probe and the remaining chunks are planned from the
       Content-Range total of its 206. */
    auto fetched = fx.fetch(server, "/big", std::nullopt);

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 4u);
    EXPECT_EQ(countRanged(reqs), 4u);
    EXPECT_EQ(reqs[0].rangeHeader, std::optional<std::string>("bytes=0-1048575"));
    for (auto & r : reqs)
        if (r.rangeHeader != "bytes=0-1048575")
            EXPECT_EQ(r.ifMatchHeader, std::optional<std::string>("\"v1\""));
}

TEST(ChunkedDownload, unknownSizeSmallFileIsOneProbe)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(MiB / 2);
    HttpRangeServer server{{{"/small", body}}};

    /* The probe's 206 already covers the whole file: no further requests. */
    auto fetched = fx.fetch(server, "/small", std::nullopt);

    EXPECT_EQ(fetched, body);
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ChunkedDownload, unknownSizeFallsBackWhenServerIgnoresRanges)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}, {.supportRanges = false, .advertiseRanges = false}};

    auto fetched = fx.fetch(server, "/big", std::nullopt);

    EXPECT_EQ(fetched, body);
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ChunkedDownload, unknownSizeWithoutRangeTotalIsDownloadedPlainly)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}, {.omitRangeTotal = true}};

    /* A 206 with `bytes a-b/\*` gives nothing to plan the remaining chunks
       from: the probe is abandoned and the file fetched unchunked. */
    auto fetched = fx.fetch(server, "/big", std::nullopt);

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    ASSERT_EQ(reqs.size(), 2u);
    EXPECT_TRUE(reqs[0].rangeHeader.has_value());
    EXPECT_FALSE(reqs[1].rangeHeader.has_value());
}

/* The size hint (a .narinfo's FileSize) is untrusted: cache.nixos.org has
   served NARs a few bytes off from their FileSize. Chunks must be planned
   from the server's Content-Range total, in either direction of error. */
TEST(ChunkedDownload, wrongSizeHintIsIgnored)
{
    auto body = makeBody(3 * MiB + 4321);
    for (uint64_t hint : {body.size() - 7, uint64_t{MiB + 1}, body.size() + 7, uint64_t{10 * MiB}}) {
        SCOPED_TRACE(hint);
        ChunkedDownloadFixture fx;
        HttpRangeServer server{{{"/big", body}}, {.etag = "\"v1\""}};

        auto fetched = fx.fetch(server, "/big", hint);

        EXPECT_EQ(fetched, body);
        auto reqs = server.requests();
        EXPECT_EQ(reqs.size(), 4u);
        EXPECT_EQ(countRanged(reqs), 4u);
        EXPECT_TRUE(std::ranges::any_of(reqs, [&](auto & r) {
            return r.rangeHeader == "bytes=" + std::to_string(3 * MiB) + "-" + std::to_string(body.size() - 1);
        }));
    }
}

TEST(ChunkedDownload, wrongSizeHintFallsBackWhenServerIgnoresRanges)
{
    auto body = makeBody(3 * MiB);
    for (uint64_t hint : {body.size() - 7, body.size() + 7}) {
        SCOPED_TRACE(hint);
        ChunkedDownloadFixture fx;
        HttpRangeServer server{{{"/big", body}}, {.supportRanges = false, .advertiseRanges = false}};

        auto fetched = fx.fetch(server, "/big", hint);

        EXPECT_EQ(fetched, body);
        EXPECT_EQ(server.requests().size(), 1u);
    }
}

TEST(ChunkedDownload, transientChunkFailureIsRetried)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(4 * MiB);
    RangeServerBehavior behavior;
    /* Fail the third request once with a retryable status. */
    behavior.injectError = [](size_t ordinal) -> std::optional<int> {
        if (ordinal == 3)
            return 500;
        return std::nullopt;
    };
    HttpRangeServer server{{{"/big", body}}, behavior};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    /* 4 chunks + 1 retried request. */
    EXPECT_EQ(server.requests().size(), 5u);
}

TEST(ChunkedDownload, mutatedObjectAborts)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    RangeServerBehavior behavior;
    behavior.etag = "\"v1\"";
    /* Simulate the object changing under us: every non-probe chunk gets a
       precondition failure, as a real server would respond once the ETag
       no longer matches. */
    behavior.injectError = [](size_t ordinal) -> std::optional<int> {
        if (ordinal > 1)
            return 412;
        return std::nullopt;
    };
    HttpRangeServer server{{{"/big", body}}, behavior};

    EXPECT_THROW(fx.fetch(server, "/big", body.size()), FileTransferError);
}

TEST(ChunkedDownload, outOfOrderCompletionReassemblesInOrder)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(4 * MiB);
    RangeServerBehavior behavior;
    /* Stall the earliest requests so later chunks complete first; the
       sink must still receive the bytes in order. */
    behavior.delay = [](size_t ordinal) -> std::optional<std::chrono::milliseconds> {
        if (ordinal <= 2)
            return std::chrono::milliseconds(300 / ordinal);
        return std::nullopt;
    };
    HttpRangeServer server{{{"/big", body}}, behavior};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
}

TEST(ChunkedDownload, midBodyDisconnectResumesChunk)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(4 * MiB);
    RangeServerBehavior behavior;
    /* Cut one chunk's response after 1000 body bytes. The retry must
       resume from the received offset (Range arithmetic), not restart
       the whole download. */
    behavior.truncateAfter = [](size_t ordinal) -> std::optional<size_t> {
        if (ordinal == 2)
            return 1000;
        return std::nullopt;
    };
    HttpRangeServer server{{{"/big", body}}, behavior};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    /* The truncated chunk was re-requested with a narrowed Range that
       starts 1000 bytes into its original range. */
    bool sawResumedRange = false;
    for (uint64_t first = 0; first < body.size(); first += MiB) {
        auto last = std::min<uint64_t>(first + MiB, body.size()) - 1;
        auto resumed = "bytes=" + std::to_string(first + 1000) + "-" + std::to_string(last);
        for (auto & r : server.requests())
            if (r.rangeHeader == resumed)
                sawResumedRange = true;
    }
    EXPECT_TRUE(sawResumedRange);
    EXPECT_EQ(server.requests().size(), 5u);
}

TEST(ChunkedDownload, weakEtagSkipsIfMatch)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}, {.etag = "W/\"weak\""}};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    /* Weak etags never match under If-Match's strong comparison, so no
       chunk may send the header. */
    for (auto & r : server.requests())
        EXPECT_EQ(r.ifMatchHeader, std::nullopt);
}

/* Regression: cache.nixos-cuda.org (Caddy) answers `Range: bytes=0-8388607`
   with `200`, `Content-Range: bytes 0-8388607/<total>` and exactly the
   requested bytes. That is a partial response despite the status; treating
   it as a full-body 200 failed with "server sent 8388608 bytes for bytes
   0-8388607". */
TEST(ChunkedDownload, partialContentSentAs200IsAccepted)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB + 4321);
    HttpRangeServer server{{{"/big", body}}, {.partialStatus = 200}};

    auto fetched = fx.fetch(server, "/big", body.size());

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 4u);
    EXPECT_EQ(countRanged(reqs), 4u);
}

TEST(ChunkedDownload, unknownSizePartialContentSentAs200IsPlannedFromProbe)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB + 4321);
    HttpRangeServer server{{{"/big", body}}, {.partialStatus = 200}};

    auto fetched = fx.fetch(server, "/big", std::nullopt);

    EXPECT_EQ(fetched, body);
    EXPECT_EQ(countRanged(server.requests()), 4u);
}

TEST(ChunkedDownload, partialContentSentAs200ResumesChunk)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    RangeServerBehavior behavior;
    behavior.partialStatus = 200;
    /* Cut the probe; its retry asks for a narrowed range whose 200 must
       still be recognized as partial. */
    behavior.truncateAfter = [](size_t ordinal) -> std::optional<size_t> {
        if (ordinal == 1)
            return 1000;
        return std::nullopt;
    };
    HttpRangeServer server{{{"/big", body}}, behavior};

    auto fetched = fx.fetch(server, "/big", body.size());
    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 4u);
    EXPECT_TRUE(std::ranges::any_of(reqs, [](auto & r) {
        return r.rangeHeader == std::optional<std::string>("bytes=1000-1048575");
    }));
}

struct NarInfoFileFields
{
    /** Added to the real file size; nullopt omits FileSize and FileHash. */
    std::optional<int64_t> fileSizeError = 0;
    /** Whether FileHash is the hash of something else. */
    bool wrongFileHash = false;
};

/**
 * Substitute a > 1 chunk NAR from an HTTP binary cache into a local store
 * and check that the NAR itself (not the narinfo or cache-info) was
 * fetched as ranged chunks, planned from the real size. `FileSize` and
 * `FileHash` in the .narinfo are untrusted hints: whether absent or wrong,
 * substitution must succeed, the NarHash check of the local store being
 * what guarantees integrity.
 */
static void substituteChunked(NarInfoFileFields fields)
{
    ChunkedDownloadFixture fx;

    /* Build a NAR of > 1 chunk. */
    auto content = makeBody(3 * MiB);
    StringSink narSink;
    dumpString(content, narSink);
    auto nar = std::move(narSink.s);
    auto narHash = hashString(HashAlgorithm::SHA256, nar);

    /* The store dir of a config is what the .narinfo must name. */
    auto storeDir =
        make_ref<testing::TestHttpBinaryCacheStoreConfig>(parseURL("http://127.0.0.1:1"), Store::Config::Params{})
            ->storeDir;

    auto hashPart = narHash.to_string(HashFormat::Nix32, false).substr(0, 32);
    auto pathName = hashPart + "-chunked-test";

    auto fileHash = fields.wrongFileHash ? hashString(HashAlgorithm::SHA256, "not the nar") : narHash;
    auto narInfo =
        fmt("StorePath: %s/%s\n"
            "URL: nar/test.nar\n"
            "Compression: none\n"
            "%s"
            "NarHash: %s\n"
            "NarSize: %d\n"
            "References: \n",
            storeDir,
            pathName,
            fields.fileSizeError ? fmt("FileHash: %s\nFileSize: %d\n",
                                       fileHash.to_string(HashFormat::Nix32, true),
                                       static_cast<int64_t>(nar.size()) + *fields.fileSizeError)
                                 : std::string{},
            narHash.to_string(HashFormat::Nix32, true),
            nar.size());

    HttpRangeServer server{
        {{"/nix-cache-info", fmt("StoreDir: %s\n", storeDir)},
         {"/" + hashPart + ".narinfo", narInfo},
         {"/nar/test.nar", nar}}};

    auto ft = makeFileTransfer(fx.settings);
    auto config = make_ref<testing::TestHttpBinaryCacheStoreConfig>(parseURL(server.url("")), Store::Config::Params{});
    auto store = config->openTestStore(ft);
    store->init();

    AutoDelete root(canonPath(createTempDir(), /*resolveSymlinks=*/true));
    auto localConfig = make_ref<LocalStoreConfig>(root.path().string(), StoreConfig::Params{});
    ASSERT_EQ(localConfig->storeDir, storeDir);
    auto localStore = make_ref<LocalStore>(localConfig);

    StorePath path(pathName);
    copyStorePath(*store, *localStore, path, NoRepair, NoCheckSigs);

    auto info = localStore->queryPathInfo(path);
    EXPECT_EQ(info->narHash, narHash);
    EXPECT_EQ(info->narSize, nar.size());
    /* The NAR itself (3 requests of 1 MiB + change) was fetched chunked;
       narinfo and cache-info were not. */
    size_t ranged = countRanged(server.requests());
    EXPECT_EQ(ranged, (nar.size() + MiB - 1) / MiB);
}

TEST(ChunkedDownload, substitutionIsChunkedEndToEnd)
{
    substituteChunked({});
}

TEST(ChunkedDownload, substitutionWithoutFileSizeIsChunkedEndToEnd)
{
    substituteChunked({.fileSizeError = std::nullopt});
}

/* Regression: cache.nixos.org serves some NARs whose size and hash differ
   from the FileSize/FileHash of their .narinfo (e.g.
   39lyf5g2iz4jd2r6q4g5whk9vw09zglj.narinfo claims 9064108 bytes for a
   9064101-byte .nar.zst). */
TEST(ChunkedDownload, substitutionWithTooSmallFileSizeSucceeds)
{
    substituteChunked({.fileSizeError = -7, .wrongFileHash = true});
}

TEST(ChunkedDownload, substitutionWithTooLargeFileSizeSucceeds)
{
    substituteChunked({.fileSizeError = 7, .wrongFileHash = true});
}

} // namespace nix

#endif
