#include "nix/store/tests/http-range-server.hh"

#ifndef _WIN32

#  include "nix/util/error.hh"
#  include "nix/util/file-descriptor.hh"

#  include <algorithm>
#  include <cctype>
#  include <cerrno>
#  include <charconv>
#  include <cstring>
#  include <sstream>
#  include <thread>

#  include <arpa/inet.h>
#  include <sys/time.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <unistd.h>

namespace nix::test {

namespace {

std::string_view statusText(int status)
{
    switch (status) {
    case 200:
        return "OK";
    case 206:
        return "Partial Content";
    case 404:
        return "Not Found";
    case 412:
        return "Precondition Failed";
    case 416:
        return "Range Not Satisfiable";
    case 500:
        return "Internal Server Error";
    case 503:
        return "Service Unavailable";
    default:
        return "Error";
    }
}

std::string toLower(std::string_view s)
{
    std::string res{s};
    std::transform(res.begin(), res.end(), res.begin(), [](unsigned char c) { return std::tolower(c); });
    return res;
}

std::string_view trim(std::string_view s)
{
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t'))
        s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r'))
        s.remove_suffix(1);
    return s;
}

std::optional<uint64_t> parseDecimal(std::string_view s)
{
    uint64_t value;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc{} || ptr != s.data() + s.size())
        return std::nullopt;
    return value;
}

/**
 * Parse a single-range `bytes=a-b` or `bytes=a-` header value.
 *
 * Returns the closed interval [first, last] clamped to `total`, or nullopt if
 * the header is malformed or unsatisfiable (caller responds 416).
 */
std::optional<std::pair<uint64_t, uint64_t>> parseRange(std::string_view value, uint64_t total)
{
    value = trim(value);
    if (!value.starts_with("bytes="))
        return std::nullopt;
    value.remove_prefix(std::strlen("bytes="));

    auto dash = value.find('-');
    if (dash == std::string_view::npos || dash == 0)
        return std::nullopt;

    auto first = parseDecimal(value.substr(0, dash));
    if (!first || *first >= total)
        return std::nullopt;

    auto lastStr = value.substr(dash + 1);
    uint64_t last;
    if (lastStr.empty())
        last = total - 1;
    else {
        auto parsed = parseDecimal(lastStr);
        if (!parsed || *parsed < *first)
            return std::nullopt;
        last = std::min(*parsed, total - 1);
    }

    return std::pair{*first, last};
}

} // namespace

HttpRangeServer::HttpRangeServer(std::map<std::string, std::string> files, RangeServerBehavior behavior)
    : files(std::move(files))
    , behavior(std::move(behavior))
{
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0)
        throw SysError("creating HTTP range server socket");

    int on = 1;
    setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; /* ephemeral */

    if (bind(listenFd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        throw SysError("binding HTTP range server socket");

    socklen_t addrLen = sizeof(addr);
    if (getsockname(listenFd, reinterpret_cast<sockaddr *>(&addr), &addrLen) < 0)
        throw SysError("getting HTTP range server socket address");
    listenPort = ntohs(addr.sin_port);

    if (listen(listenFd, 64) < 0)
        throw SysError("listening on HTTP range server socket");

    acceptThread = std::thread([this]() { acceptLoop(); });
}

HttpRangeServer::~HttpRangeServer()
{
    stopping = true;
    shutdown(listenFd, SHUT_RDWR);
    if (acceptThread.joinable())
        acceptThread.join();
    close(listenFd);

    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(connThreadsMutex);
        threads = std::move(connThreads);
    }
    for (auto & t : threads)
        if (t.joinable())
            t.join();
}

uint16_t HttpRangeServer::port() const
{
    return listenPort;
}

std::string HttpRangeServer::url(std::string_view path) const
{
    std::ostringstream str;
    str << "http://127.0.0.1:" << listenPort << path;
    return std::move(str).str();
}

std::vector<RangeServerRequest> HttpRangeServer::requests() const
{
    std::lock_guard<std::mutex> lock(requestsMutex);
    return requestLog;
}

void HttpRangeServer::acceptLoop()
{
    while (!stopping) {
        int fd = accept(listenFd, nullptr, nullptr);
        if (fd < 0) {
            if (stopping)
                break;
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            break;
        }
        std::lock_guard<std::mutex> lock(connThreadsMutex);
        connThreads.emplace_back([this, fd]() { handleConnection(fd); });
    }
}

void HttpRangeServer::handleConnection(int fd)
{
    /* Poll with a timeout so that a pooled keep-alive connection (e.g. held
       open by curl) cannot block the destructor's join forever. */
    timeval tv{.tv_sec = 0, .tv_usec = 200 * 1000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::string buffer;

    auto readMore = [&]() -> bool {
        while (true) {
            char buf[4096];
            auto n = recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                buffer.append(buf, static_cast<size_t>(n));
                return true;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
                if (stopping)
                    return false;
                continue;
            }
            return false; /* EOF or hard error */
        }
    };

    while (true) {
        /* Read until we have a full request header block. */
        size_t headerEnd;
        while ((headerEnd = buffer.find("\r\n\r\n")) == std::string::npos) {
            if (!readMore())
                goto done;
        }

        std::string head = buffer.substr(0, headerEnd);
        buffer.erase(0, headerEnd + 4);

        /* Parse the request line. */
        RangeServerRequest req;
        bool closeConnection = false;
        {
            auto lineEnd = head.find("\r\n");
            std::string_view requestLine{head.data(), lineEnd == std::string::npos ? head.size() : lineEnd};
            auto sp1 = requestLine.find(' ');
            auto sp2 = requestLine.rfind(' ');
            if (sp1 == std::string_view::npos || sp2 <= sp1)
                break; /* malformed; drop the connection */
            req.method = std::string{requestLine.substr(0, sp1)};
            req.path = std::string{requestLine.substr(sp1 + 1, sp2 - sp1 - 1)};

            /* Parse headers. */
            size_t pos = lineEnd == std::string::npos ? head.size() : lineEnd + 2;
            while (pos < head.size()) {
                auto eol = head.find("\r\n", pos);
                if (eol == std::string::npos)
                    eol = head.size();
                std::string_view line{head.data() + pos, eol - pos};
                pos = eol + 2;
                auto colon = line.find(':');
                if (colon == std::string_view::npos)
                    continue;
                auto name = toLower(trim(line.substr(0, colon)));
                auto value = std::string{trim(line.substr(colon + 1))};
                if (name == "range")
                    req.rangeHeader = value;
                else if (name == "if-match")
                    req.ifMatchHeader = value;
                else if (name == "connection" && toLower(value) == "close")
                    closeConnection = true;
            }
        }

        auto ordinal = ++requestCounter;

        {
            std::lock_guard<std::mutex> lock(requestsMutex);
            requestLog.push_back(req);
        }

        /* Compute status, headers and body. */
        int status = 200;
        std::string extraHeaders;
        std::string_view body;

        std::optional<int> injected;
        if (behavior.injectError)
            injected = behavior.injectError(ordinal);

        if (injected) {
            status = *injected;
        } else if (req.method != "GET" && req.method != "HEAD") {
            status = 404;
        } else if (auto file = files.find(req.path); file == files.end()) {
            status = 404;
        } else if (req.ifMatchHeader && !behavior.etag.empty() && *req.ifMatchHeader != behavior.etag) {
            status = 412;
        } else {
            const std::string & contents = file->second;
            body = contents;
            if (behavior.supportRanges && req.rangeHeader) {
                if (auto range = parseRange(*req.rangeHeader, contents.size())) {
                    auto [first, last] = *range;
                    status = 206;
                    extraHeaders += "Content-Range: bytes " + std::to_string(first) + "-" + std::to_string(last) + "/"
                                    + std::to_string(contents.size()) + "\r\n";
                    body = std::string_view{contents}.substr(first, last - first + 1);
                } else {
                    status = 416;
                    extraHeaders += "Content-Range: bytes */" + std::to_string(contents.size()) + "\r\n";
                    body = {};
                }
            }
        }

        std::string response = "HTTP/1.1 " + std::to_string(status) + " " + std::string{statusText(status)} + "\r\n";
        if (behavior.advertiseRanges)
            response += "Accept-Ranges: bytes\r\n";
        if (!behavior.etag.empty())
            response += "ETag: " + behavior.etag + "\r\n";
        response += extraHeaders;
        response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
        response += "\r\n";

        std::optional<size_t> truncateAt;
        if (behavior.truncateAfter && req.method != "HEAD")
            truncateAt = behavior.truncateAfter(ordinal);

        if (req.method != "HEAD")
            response += truncateAt ? body.substr(0, std::min(*truncateAt, body.size())) : body;

        if (behavior.delay)
            if (auto d = behavior.delay(ordinal))
                std::this_thread::sleep_for(*d);

        /* send() with MSG_NOSIGNAL rather than writeFull(): a plain
           write() to a socket the peer has reset raises SIGPIPE, which
           would kill the whole test process. */
        auto sendAll = [&](std::string_view data) -> bool {
            while (!data.empty()) {
                auto n = send(fd, data.data(), data.size(), MSG_NOSIGNAL);
                if (n < 0) {
                    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;
                    return false;
                }
                data.remove_prefix(static_cast<size_t>(n));
            }
            return true;
        };

        if (!sendAll(response))
            break; /* peer went away */

        if (truncateAt)
            break; /* simulate a mid-body disconnect */

        if (closeConnection)
            break;
    }

done:
    close(fd);
}

} // namespace nix::test

#endif
