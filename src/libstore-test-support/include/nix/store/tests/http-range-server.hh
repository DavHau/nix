#pragma once
///@file

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace nix::test {

struct RangeServerBehavior
{
    /** Whether to honor Range requests with 206; false → always 200 full body. */
    bool supportRanges = true;
    /** ETag sent with every response; empty → no ETag header. */
    std::string etag;
    /** Whether to advertise `Accept-Ranges: bytes`. */
    bool advertiseRanges = true;
    /** Send `Content-Range: bytes a-b/\*` (unknown total) on 206 responses,
        as some dynamically generated content does. */
    bool omitRangeTotal = false;
    /** Called with the 1-based request ordinal before handling; return an HTTP
        status (e.g. 500) to short-circuit with an empty error body, or nullopt
        to handle normally. Used for transient-failure injection. */
    std::function<std::optional<int>(size_t requestOrdinal)> injectError;
    /** Called with the 1-based request ordinal; a returned duration is
        slept before the response is written. Used to force out-of-order
        chunk completion. */
    std::function<std::optional<std::chrono::milliseconds>(size_t requestOrdinal)> delay;
    /** Called with the 1-based request ordinal; when it returns a byte
        count, only that many body bytes are written (Content-Length still
        advertises the full length) and the connection is closed. Used to
        exercise mid-body disconnects and retry-resume. */
    std::function<std::optional<size_t>(size_t requestOrdinal)> truncateAfter;
};

struct RangeServerRequest
{
    std::string method;
    std::string path;
    /** Raw value of the Range: header, if present. */
    std::optional<std::string> rangeHeader;
    /** Raw value of the If-Match: header, if present. */
    std::optional<std::string> ifMatchHeader;
};

/**
 * A minimal HTTP/1.1 server for exercising byte-range (chunked parallel
 * download) code paths in libstore. Serves a fixed set of in-memory files on
 * 127.0.0.1 with an ephemeral port, supporting GET/HEAD, single `bytes=a-b`
 * ranges, ETag/If-Match, keep-alive, concurrent connections and per-request
 * error injection.
 */
class HttpRangeServer
{
public:
    /** Start serving on 127.0.0.1 on an ephemeral port. `files` maps URL path
        (e.g. "/nar/big.nar") to full body. */
    HttpRangeServer(std::map<std::string, std::string> files, RangeServerBehavior behavior = {});

    /** Stops the accept loop and joins all connection threads. */
    ~HttpRangeServer();

    HttpRangeServer(const HttpRangeServer &) = delete;
    HttpRangeServer & operator=(const HttpRangeServer &) = delete;

    uint16_t port() const;

    /** Returns "http://127.0.0.1:<port><path>". */
    std::string url(std::string_view path) const;

    /** Thread-safe snapshot of all requests received so far. */
    std::vector<RangeServerRequest> requests() const;

private:
    std::map<std::string, std::string> files;
    RangeServerBehavior behavior;

    int listenFd = -1;
    uint16_t listenPort = 0;

    std::atomic<bool> stopping{false};

    std::thread acceptThread;
    std::mutex connThreadsMutex;
    std::vector<std::thread> connThreads;

    std::atomic<size_t> requestCounter{0};

    mutable std::mutex requestsMutex;
    std::vector<RangeServerRequest> requestLog;

    void acceptLoop();
    void handleConnection(int fd);
};

} // namespace nix::test
