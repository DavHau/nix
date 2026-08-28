#ifndef _WIN32

#  include <algorithm>
#  include <random>

#  include <gtest/gtest.h>

#  include "nix/store/filetransfer.hh"
#  include "nix/store/tests/http-range-server.hh"
#  include "nix/store/tests/https-store.hh"
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
        request.expectedSize = expectedSize;
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

TEST(ChunkedDownload, unknownSizeIsNotChunked)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}};

    auto fetched = fx.fetch(server, "/big", std::nullopt);

    EXPECT_EQ(fetched, body);
    auto reqs = server.requests();
    EXPECT_EQ(reqs.size(), 1u);
    EXPECT_EQ(countRanged(reqs), 0u);
}

TEST(ChunkedDownload, sizeMismatchAborts)
{
    ChunkedDownloadFixture fx;
    auto body = makeBody(3 * MiB);
    HttpRangeServer server{{{"/big", body}}};

    /* The cache metadata claims a different size than the server reports in
       Content-Range: refuse to assemble a corrupt stream. */
    EXPECT_THROW(fx.fetch(server, "/big", body.size() + 1000), Error);
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

TEST(ChunkedDownload, narFromPathIsChunkedEndToEnd)
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

    auto narInfo =
        fmt("StorePath: %s/%s\n"
            "URL: nar/test.nar\n"
            "Compression: none\n"
            "FileHash: %s\n"
            "FileSize: %d\n"
            "NarHash: %s\n"
            "NarSize: %d\n"
            "References: \n",
            storeDir,
            pathName,
            narHash.to_string(HashFormat::Nix32, true),
            nar.size(),
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

    StringSink fetched;
    store->narFromPath(StorePath(pathName), fetched);

    EXPECT_EQ(fetched.s, nar);
    /* The NAR itself (3 requests of 1 MiB + change) was fetched chunked;
       narinfo and cache-info were not. */
    size_t ranged = countRanged(server.requests());
    EXPECT_EQ(ranged, (nar.size() + MiB - 1) / MiB);
}

} // namespace nix

#endif
