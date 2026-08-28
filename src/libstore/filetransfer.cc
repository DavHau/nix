#include "nix/store/filetransfer.hh"
#include "nix/store/filetransfer-impl.hh"
#include "nix/store/globals.hh"
#include "nix/util/config-global.hh"
#include "nix/util/finally.hh"
#include "nix/util/callback.hh"
#include "nix/util/signals.hh"
#include "nix/util/util.hh"

#include "nix/store/s3-url.hh"
#include <optional>
#if NIX_WITH_AWS_AUTH
#  include "nix/store/aws-creds.hh"
#endif

#ifdef __linux__
#  include "nix/util/linux-namespaces.hh"
#endif

#include <unistd.h>
#include <fcntl.h>

#include <curl/curl.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <queue>
#include <random>
#include <thread>
#include <utility>
#include <regex>

namespace nix {

namespace {

enum struct HttpStatus : long {
    Ok = 200,
    Created = 201,
    NoContent = 204,
    PartialContent = 206,
    NotModified = 304,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    ProxyAuthRequired = 407,
    RequestTimeout = 408,
    Gone = 410,
    TooManyRequests = 429,
    NotImplemented = 501,
    ServiceUnavailable = 503,
    HttpVersionNotSupported = 505,
    NetworkAuthRequired = 511,
};

constexpr bool operator==(long lhs, HttpStatus rhs) noexcept
{
    return lhs == static_cast<long>(rhs);
}

} // namespace

std::chrono::milliseconds computeRetryDelayMs(const RetryDelayParams & p, std::mt19937 & rng)
{
    uint32_t backoff = clampedExponential(p.baseMs, p.attempt, p.ceilMs);

    // Retry-After is a hard minimum — the server explicitly asked us to wait
    // at least this long. ceilMs caps the backoff algorithm, not the server's
    // signal: retrying before the server is ready just burns an attempt.
    uint32_t floor = p.retryAfterMs.value_or(0);

    if (!p.jitter)
        return std::chrono::milliseconds(std::max(floor, backoff));

    // Jitter spreads retries over [floor, floor+backoff] so that concurrent
    // clients receiving the same Retry-After don't all retry simultaneously.
    // Saturating add: clamp if floor + backoff would overflow uint32_t.
    constexpr auto u32max = std::numeric_limits<uint32_t>::max();
    uint32_t ceiling = (backoff > u32max - floor) ? u32max : floor + backoff;
    if (ceiling <= floor)
        return std::chrono::milliseconds(floor);

    return std::chrono::milliseconds(std::uniform_int_distribution<uint32_t>(floor, ceiling)(rng));
}

std::optional<std::filesystem::path> FileTransferSettings::getDefaultSSLCertFile()
{
    /* Windows has no notion of a default location for the certificate bundles.
       Instead we use CURLSSLOPT_NATIVE_CA by default. */
#ifndef _WIN32
    for (auto & fn :
         {"/etc/ssl/certs/ca-certificates.crt", "/nix/var/nix/profiles/default/etc/ssl/certs/ca-bundle.crt"})
        if (pathAccessible(fn))
            return fn;
#endif
    return std::nullopt;
}

void FileTransferSettings::anchor() {}

FileTransferSettings::FileTransferSettings()
{
    std::optional<AbsolutePath> sslOverride =
        getEnvOs(OS_STR("NIX_SSL_CERT_FILE"))
            .or_else([] { return getEnvOs(OS_STR("SSL_CERT_FILE")); })
            .and_then([](OsString s) -> std::optional<OsString> {
                return s.empty() ? std::nullopt : std::optional{std::move(s)};
            })
            .transform([](OsString s) { return AbsolutePath{std::filesystem::path{std::move(s)}}; });
    if (sslOverride)
        caFile = *sslOverride;
}

FileTransferSettings fileTransferSettings;

static GlobalConfig::Register rFileTransferSettings(&fileTransferSettings);

FileTransfer::~FileTransfer() {}

namespace {

using curlSList = std::unique_ptr<::curl_slist, decltype([](::curl_slist * list) { ::curl_slist_free_all(list); })>;
using curlMulti = std::unique_ptr<::CURLM, decltype([](::CURLM * multi) { ::curl_multi_cleanup(multi); })>;

struct curlMultiError final : CloneableError<curlMultiError, Error>
{
    ::CURLMcode code;

    curlMultiError(::CURLMcode code)
        : CloneableError{"unexpected curl multi error: %s", ::curl_multi_strerror(code)}
    {
        assert(code != CURLM_OK);
    }
};

/* Check if the linked libcurl was built with HTTP3 support. */
bool curlSupportsHttp3()
{
    const auto * info = ::curl_version_info(CURLVERSION_NOW);
    return info && (info->features & CURL_VERSION_HTTP3);
}

} // namespace

struct curlFileTransfer : public FileTransfer
{
    const FileTransferSettings & settings;

    const bool http3Supported = curlSupportsHttp3();

    curlMulti curlm;

    std::random_device rd;
    std::mt19937 mt19937;

    struct TransferItem : public std::enable_shared_from_this<TransferItem>, public FileTransfer::Item
    {
        curlFileTransfer & fileTransfer;
        FileTransferRequest request;
        FileTransferResult result;
        std::unique_ptr<Activity> _act;
        Callback<FileTransferResult> callback;
        CURL * req = 0;
        // buffer to accompany the `req` above
        char errbuf[CURL_ERROR_SIZE];
        std::string statusMsg;

        uint32_t attempt = 0;

        /* Don't start this download until the specified time point
           has been reached. */
        std::chrono::steady_clock::time_point embargo;

        curlSList requestHeaders;

        /**
         * Whether either the success or failure function has been called.
         */
        bool done:1 = false;

        /**
         * Whether the handle has been added to the multi object.
         */
        bool active:1 = false;

        /**
         * Whether the request has been paused previously.
         */
        bool paused:1 = false;

        /**
         * Whether the request has been added the incoming queue.
         */
        bool enqueued:1 = false;

        /**
         * Whether we can use range downloads for retries.
         */
        bool acceptRanges:1 = false;

        /**
         * Whether the response has a non-trivial (not "identity") Content-Encoding.
         */
        bool hasContentEncoding:1 = false;

        /**
         * Whether request.responseCallback has been invoked for the
         * current response.
         */
        bool responseNotified:1 = false;

        /**
         * Total object size parsed from a `Content-Range` response header
         * (the `<total>` in `bytes <a>-<b>/<total>`), if present.
         */
        std::optional<uint64_t> contentRangeTotal;

        /**
         * Server-provided minimum retry delay, parsed from the `Retry-After`
         * response header. Reset on each new HTTP status line, and consumed
         * (cleared) by maybeRetry() so it applies to at most one retry attempt.
         */
        std::optional<uint32_t> retryAfterMs;

        curl_off_t writtenToSink = 0;

        std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();

        inline static const std::set<long> successfulStatuses{
            static_cast<long>(HttpStatus::Ok),
            static_cast<long>(HttpStatus::Created),
            static_cast<long>(HttpStatus::NoContent),
            static_cast<long>(HttpStatus::PartialContent),
            static_cast<long>(HttpStatus::NotModified),
            0 /* other protocol */};

        /* Get the HTTP status code, or 0 for other protocols. */
        long getHTTPStatus()
        {
            long httpStatus = 0;
            long protocol = 0;
            curl_easy_getinfo(req, CURLINFO_PROTOCOL, &protocol);
            if (protocol == CURLPROTO_HTTP || protocol == CURLPROTO_HTTPS)
                curl_easy_getinfo(req, CURLINFO_RESPONSE_CODE, &httpStatus);
            return httpStatus;
        }

        void appendHeaders(const std::string & header)
        {
            curlSList tmpSList = curlSList(::curl_slist_append(requestHeaders.get(), requireCString(header)));
            if (!tmpSList)
                throw std::bad_alloc();
            requestHeaders.release();
            requestHeaders = std::move(tmpSList);
        }

        TransferItem(
            curlFileTransfer & fileTransfer, FileTransferRequest request, Callback<FileTransferResult> && callback)
            : fileTransfer(fileTransfer)
            , request(std::move(request))
            , callback(std::move(callback))
            , finalSink([this](std::string_view data) {
                if (errorSink) {
                    (*errorSink)(data);
                }

                if (this->request.dataCallback) {
                    auto httpStatus = getHTTPStatus();

                    /* Only write data to the sink if this is a
                       successful response. */
                    if (successfulStatuses.count(httpStatus)) {
                        writtenToSink += data.size();
                        PauseTransfer needsPause = this->request.dataCallback(data);
                        if (needsPause == PauseTransfer::Yes) {
                            /* Smuggle the boolean flag into writeCallback. Note that
                               the finalSink might get called multiple times if there's
                               decompression going on. */
                            paused = true;
                        }
                    }
                } else
                    this->result.data.append(data);
            })
        {
            /* NOTE: the `request` parameter is moved-from; only the member
               (`this->request`) is valid here. */
            result.urls.push_back(this->request.uri.to_string());

            if (!this->request.expectedETag.empty())
                appendHeaders("If-None-Match: " + this->request.expectedETag);
            if (!this->request.mimeType.empty())
                appendHeaders("Content-Type: " + this->request.mimeType);
            for (auto & [name, value] : this->request.headers)
                appendHeaders(fmt("%s: %s", name, value));
        }

        ~TransferItem()
        {
            if (req) {
                if (active) {
                    curl_multi_remove_handle(fileTransfer.curlm.get(), req);
                    fileTransfer.activeHandles--;
                }
                curl_easy_cleanup(req);
            }
            try {
                if (!done && enqueued)
                    failInterruptedOrCancelled();
            } catch (...) {
                ignoreExceptionInDestructor();
            }
        }

        void failEx(std::exception_ptr ex) noexcept
        {
            assert(!done);
            done = true;
            try {
                std::rethrow_exception(ex);
            } catch (FileTransferError & e) {
                /* Already descriptive enough. */
            } catch (nix::Error & e) {
                /* Add more context to the error message. */
                e.addTrace({}, "during %s of '%s'", Uncolored(request.noun()), request.displayUri());
            } catch (...) {
                /* Can't add more context to the error. */
            }
            callback.rethrow(ex);
        }

        template<class T>
        void fail(T && e) noexcept
        {
            failEx(std::make_exception_ptr(std::forward<T>(e)));
        }

        void failInterruptedOrCancelled()
        {
            HintFmt fmt("%s of '%s' was interrupted", Uncolored(request.noun()), request.displayUri());

            /* Technically, we don't really have per-transfer cancellation currently,
               but it's nice to distinguish between the two in the future. */
            if (getInterrupted())
                fail(nix::Interrupted(std::move(fmt)));
            else
                fail(nix::Cancelled(std::move(fmt)));
        }

        LambdaSink finalSink;
        std::optional<StringSink> errorSink;

        std::exception_ptr callbackException;

        size_t writeCallback(void * contents, size_t size, size_t nmemb) noexcept
        try {
            size_t realSize = size * nmemb;
            result.bodySize += realSize;

            if (!errorSink && !successfulStatuses.count(getHTTPStatus())) {
                // In this case we want to construct a TeeSink, to keep
                // the response around (which we figure won't be big
                // like an actual download should be) to improve error
                // messages.
                errorSink = StringSink{};
            }

            maybeNotifyResponse();

            finalSink({static_cast<const char *>(contents), realSize});
            if (paused) {
                /* The callback has signaled that the transfer needs to be
                   paused. Already consumed data won't be returned twice unlike
                   when returning CURL_WRITEFUNC_PAUSE.
                   https://curl-library.cool.haxx.narkive.com/larE1cRA/curl-easy-pause-documentation-question
                   */
                curl_easy_pause(req, CURLPAUSE_RECV);
            }

            return realSize;
        } catch (...) {
            callbackException = std::current_exception();
            return 0;
        }

        static size_t writeCallbackWrapper(void * contents, size_t size, size_t nmemb, void * userp)
        {
            return ((TransferItem *) userp)->writeCallback(contents, size, nmemb);
        }

        /**
         * Once per response, as soon as a successful status is known (the
         * first body bytes, or completion for empty bodies): update
         * range-related state and invoke request.responseCallback if set.
         * Runs on the transfer thread.
         */
        void maybeNotifyResponse()
        {
            if (responseNotified)
                return;
            auto httpStatus = getHTTPStatus();
            if (!successfulStatuses.count(httpStatus))
                return;
            responseNotified = true;
            if (httpStatus == std::to_underlying(HttpStatus::PartialContent))
                /* A 206 is direct evidence of range support, even if the
                   server didn't send `Accept-Ranges` (needed for resuming
                   ranged chunks after transient failures). */
                acceptRanges = true;
            else if (request.byteRange)
                /* The server ignored our Range request and is sending the
                   full body. Clear the range so any retry of this item
                   resumes as a plain download instead of re-requesting a
                   range the server doesn't honor. */
                request.byteRange.reset();
            if (request.responseCallback)
                request.responseCallback(httpStatus, contentRangeTotal, result.etag);
        }

        void appendCurrentUrl()
        {
            char * effectiveUriCStr = nullptr;
            curl_easy_getinfo(req, CURLINFO_EFFECTIVE_URL, &effectiveUriCStr);
            if (effectiveUriCStr && *result.urls.rbegin() != effectiveUriCStr)
                result.urls.push_back(effectiveUriCStr);
        }

        size_t headerCallback(void * contents, size_t size, size_t nmemb) noexcept
        try {
            size_t realSize = size * nmemb;
            std::string line((char *) contents, realSize);
            printMsg(lvlVomit, "got header for '%s': %s", request.displayUri(), trim(line));

            static std::regex statusLine("HTTP/[^ ]+ +[0-9]+(.*)", std::regex::extended | std::regex::icase);
            if (std::smatch match; std::regex_match(line, match, statusLine)) {
                result.etag = "";
                result.data.clear();
                result.bodySize = 0;
                statusMsg = trim(match.str(1));
                acceptRanges = false;
                hasContentEncoding = false;
                retryAfterMs = std::nullopt;
                responseNotified = false;
                contentRangeTotal = std::nullopt;
                appendCurrentUrl();
            } else {

                auto i = line.find(':');
                if (i != std::string::npos) {
                    std::string name = toLower(trim(line.substr(0, i)));

                    if (name == "etag") {
                        result.etag = trim(line.substr(i + 1));
                        /* Hack to work around a GitHub bug: it sends
                           ETags, but ignores If-None-Match. So if we get
                           the expected ETag on a 200 response, then shut
                           down the connection because we already have the
                           data. */
                        long httpStatus = 0;
                        curl_easy_getinfo(req, CURLINFO_RESPONSE_CODE, &httpStatus);
                        if (result.etag == request.expectedETag && httpStatus == HttpStatus::Ok) {
                            debug("shutting down on 200 HTTP response with expected ETag");
                            return 0;
                        }
                    }

                    else if (name == "content-encoding") {
                        auto value = toLower(trim(line.substr(i + 1)));
                        hasContentEncoding = !value.empty() && value != "identity";
                    }

                    else if (name == "accept-ranges" && toLower(trim(line.substr(i + 1))) == "bytes")
                        acceptRanges = true;

                    else if (name == "content-range") {
                        // `bytes <first>-<last>/<total>`; only the total is of
                        // interest (sanity-checking chunked downloads).
                        auto value = trim(line.substr(i + 1));
                        static std::regex contentRangeRegex(
                            "bytes +[0-9]+-[0-9]+/([0-9]+)", std::regex::extended | std::regex::icase);
                        if (std::smatch match; std::regex_match(value, match, contentRangeRegex))
                            contentRangeTotal = string2Int<uint64_t>(match.str(1));
                    }

                    else if (name == "link" || name == "x-amz-meta-link") {
                        auto value = trim(line.substr(i + 1));
                        static std::regex linkRegex(
                            "<([^>]*)>; rel=\"immutable\"", std::regex::extended | std::regex::icase);
                        if (std::smatch match; std::regex_match(value, match, linkRegex))
                            result.immutableUrl = match.str(1);
                        else
                            debug("got invalid link header '%s'", value);
                    }

                    else if (name == "retry-after") {
                        auto value = trim(line.substr(i + 1));
                        // RFC 7231 §7.1.3: Retry-After = HTTP-date / delay-seconds.
                        if (auto seconds = string2Int<uint32_t>(value)) {
                            retryAfterMs = saturateMs(std::chrono::seconds{*seconds});
                        } else if (time_t date = curl_getdate(requireCString(value), nullptr); date != -1) {
                            time_t now = time(nullptr);
                            retryAfterMs = saturateMs(std::chrono::seconds{date > now ? date - now : 0});
                        } else {
                            debug("ignoring unparseable Retry-After header: '%s'", value);
                        }
                    }
                }
            }
            return realSize;
        } catch (...) {
            /* https://curl.se/libcurl/c/CURLOPT_HEADERFUNCTION.html:
               You can also abort the transfer by returning CURL_WRITEFUNC_ERROR. */
            callbackException = std::current_exception();
            return CURL_WRITEFUNC_ERROR;
        }

        static size_t headerCallbackWrapper(void * contents, size_t size, size_t nmemb, void * userp)
        {
            return ((TransferItem *) userp)->headerCallback(contents, size, nmemb);
        }

        /**
         * Lazily start an `Activity`. We don't do this in the `TransferItem` constructor to avoid showing downloads
         * that are only enqueued but not actually started.
         */
        Activity & act()
        {
            if (!_act) {
                _act = std::make_unique<Activity>(
                    *logger,
                    lvlTalkative,
                    actFileTransfer,
                    fmt("%s '%s'", request.verb(/*continuous=*/true), request.displayUri()),
                    Logger::Fields{request.displayUri()},
                    request.parentAct);
                // Reset the start time to when we actually started the download.
                startTime = std::chrono::steady_clock::now();
            }
            return *_act;
        }

        int progressCallback(curl_off_t dltotal, curl_off_t dlnow) noexcept
        try {
            act().progress(dlnow, dltotal);
            return getInterrupted();
        } catch (nix::Interrupted &) {
            assert(getInterrupted());
            return 1;
        } catch (...) {
            /* Something unexpected has happened like logger throwing an exception. */
            callbackException = std::current_exception();
            return 1;
        }

        static int progressCallbackWrapper(
            void * userp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
        {
            auto & item = *static_cast<TransferItem *>(userp);
            auto isUpload = bool(item.request.data);
            return item.progressCallback(isUpload ? ultotal : dltotal, isUpload ? ulnow : dlnow);
        }

        static int debugCallback(CURL * handle, curl_infotype type, char * data, size_t size, void * userptr) noexcept
        try {
            if (type == CURLINFO_TEXT)
                vomit("curl: %s", chomp(std::string(data, size)));
            return 0;
        } catch (...) {
            /* Swallow the exception. Nothing left to do. */
            return 0;
        }

        size_t readCallback(char * buffer, size_t size, size_t nitems) noexcept
        try {
            auto data = request.data;
            return data->source->read(buffer, nitems * size);
        } catch (EndOfFile &) {
            return 0;
        } catch (...) {
            callbackException = std::current_exception();
            return CURL_READFUNC_ABORT;
        }

        static size_t readCallbackWrapper(char * buffer, size_t size, size_t nitems, void * userp) noexcept
        {
            return ((TransferItem *) userp)->readCallback(buffer, size, nitems);
        }

#if !defined(_WIN32)
        static int cloexec_callback(void *, curl_socket_t curlfd, curlsocktype purpose)
        {
            unix::closeOnExec(curlfd);
            vomit("cloexec set for fd %i", curlfd);
            return CURL_SOCKOPT_OK;
        }
#endif

        size_t seekCallback(curl_off_t offset, int origin) noexcept
        try {
            auto source = request.data->source;
            if (origin == SEEK_SET) {
                source->restart();
                source->skip(offset);
            } else if (origin == SEEK_CUR) {
                source->skip(offset);
            } else if (origin == SEEK_END) {
                NullSink sink{};
                source->drainInto(sink);
            }
            return CURL_SEEKFUNC_OK;
        } catch (...) {
            callbackException = std::current_exception();
            return CURL_SEEKFUNC_FAIL;
        }

        static size_t seekCallbackWrapper(void * clientp, curl_off_t offset, int origin) noexcept
        {
            return ((TransferItem *) clientp)->seekCallback(offset, origin);
        }

        static int resolverCallbackWrapper(void *, void *, void * clientp) noexcept
        try {
            // Create the `Activity` associated with this download.
            ((TransferItem *) clientp)->act();
            return 0;
        } catch (...) {
            return 1;
        }

        void unpause()
        {
            /* Unpausing an already unpaused transfer is a no-op. */
            if (paused) {
                curl_easy_pause(req, CURLPAUSE_CONT);
                paused = false;
            }
        }

        void init()
        {
            if (!req)
                req = curl_easy_init();

            curl_easy_reset(req);

            if (verbosity >= lvlVomit) {
                curl_easy_setopt(req, CURLOPT_VERBOSE, 1);
                curl_easy_setopt(req, CURLOPT_DEBUGFUNCTION, TransferItem::debugCallback);
            }

            curl_easy_setopt(req, CURLOPT_URL, request.uri.to_string().c_str());

            /* Enable transparent decompression for downloads.
               Skip for uploads (Accept-Encoding is meaningless when sending data),
               when resuming from an offset, and for ranged chunks (byte ranges
               apply to the encoded stream, so they don't mix with transparent
               compression). */
            if (writtenToSink == 0 && !request.data && !request.byteRange)
                /* Empty string means to enable all supported (that libcurl has
                   been linked to support) encodings. */
                curl_easy_setopt(req, CURLOPT_ACCEPT_ENCODING, "");

            curl_easy_setopt(req, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(req, CURLOPT_MAXREDIRS, 10);
            curl_easy_setopt(req, CURLOPT_NOSIGNAL, 1);
            curl_easy_setopt(
                req,
                CURLOPT_USERAGENT,
                ("curl/" LIBCURL_VERSION " Nix/" + nixVersion
                 + (fileTransfer.settings.userAgentSuffix != "" ? " " + fileTransfer.settings.userAgentSuffix.get()
                                                                : ""))
                    .c_str());
            curl_easy_setopt(req, CURLOPT_PIPEWAIT, 1);
            /* Enable HTTP3 only on user config and linked libcurl have support, o.w. fall back. */
            if (fileTransfer.settings.enableHttp3 && fileTransfer.http3Supported)
                curl_easy_setopt(req, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_3);
            else if (fileTransfer.settings.enableHttp2)
                curl_easy_setopt(req, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
            else
                curl_easy_setopt(req, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
            curl_easy_setopt(req, CURLOPT_WRITEFUNCTION, TransferItem::writeCallbackWrapper);
            curl_easy_setopt(req, CURLOPT_WRITEDATA, this);
            curl_easy_setopt(req, CURLOPT_HEADERFUNCTION, TransferItem::headerCallbackWrapper);
            curl_easy_setopt(req, CURLOPT_HEADERDATA, this);

            curl_easy_setopt(req, CURLOPT_XFERINFOFUNCTION, progressCallbackWrapper);
            curl_easy_setopt(req, CURLOPT_XFERINFODATA, this);
            curl_easy_setopt(req, CURLOPT_NOPROGRESS, 0);

            curl_easy_setopt(req, CURLOPT_HTTPHEADER, requestHeaders.get());

            if (fileTransfer.settings.downloadSpeed.get() > 0)
                curl_easy_setopt(
                    req, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t) (fileTransfer.settings.downloadSpeed.get() * 1024));

            if (request.method == HttpMethod::Head)
                curl_easy_setopt(req, CURLOPT_NOBODY, 1);

            if (request.method == HttpMethod::Delete)
                curl_easy_setopt(req, CURLOPT_CUSTOMREQUEST, "DELETE");

            if (request.data) {
                // Restart the source to ensure it's at the beginning.
                // This is necessary for retries, where the source was
                // already consumed by a previous attempt.
                request.data->source->restart();

                if (request.method == HttpMethod::Post) {
                    curl_easy_setopt(req, CURLOPT_POST, 1L);
                    curl_easy_setopt(req, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t) request.data->sizeHint);
                } else if (request.method == HttpMethod::Put) {
                    curl_easy_setopt(req, CURLOPT_UPLOAD, 1L);
                    curl_easy_setopt(req, CURLOPT_INFILESIZE_LARGE, (curl_off_t) request.data->sizeHint);
                } else {
                    unreachable();
                }
                curl_easy_setopt(req, CURLOPT_READFUNCTION, readCallbackWrapper);
                curl_easy_setopt(req, CURLOPT_READDATA, this);
                curl_easy_setopt(req, CURLOPT_SEEKFUNCTION, seekCallbackWrapper);
                curl_easy_setopt(req, CURLOPT_SEEKDATA, this);
            }

            /* Note: libcurl copies string arguments, so temporaries from
               .string().c_str() are safe. See the comment near CURLOPT_SSLKEY below. */
            if (auto & caFile = fileTransfer.settings.caFile.get())
                curl_easy_setopt(req, CURLOPT_CAINFO, caFile->string().c_str());
#ifdef _WIN32
            /* Use native windows certificate store when the option is not specified explicitly. */
            else
                curl_easy_setopt(req, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NATIVE_CA);
#endif

#if !defined(_WIN32)
            curl_easy_setopt(req, CURLOPT_SOCKOPTFUNCTION, cloexec_callback);
#endif
            curl_easy_setopt(req, CURLOPT_CONNECTTIMEOUT, fileTransfer.settings.connectTimeout.get());

            /* Enable TCP keepalive to detect dead connections and server closures.
               Probes every 30s to catch network failures and idle timeouts early. */
            curl_easy_setopt(req, CURLOPT_TCP_KEEPALIVE, 1L);
            curl_easy_setopt(req, CURLOPT_TCP_KEEPIDLE, 30L);
            curl_easy_setopt(req, CURLOPT_TCP_KEEPINTVL, 30L);
            /* Don't reuse idle connections older than 90s. */
            curl_easy_setopt(req, CURLOPT_MAXAGE_CONN, 90L);

            curl_easy_setopt(req, CURLOPT_LOW_SPEED_LIMIT, 1L);
            curl_easy_setopt(req, CURLOPT_LOW_SPEED_TIME, fileTransfer.settings.stalledDownloadTimeout.get());

            /* If no file exist in the specified path, curl continues to work
               anyway as if netrc support was disabled. */
            curl_easy_setopt(req, CURLOPT_NETRC_FILE, fileTransfer.settings.netrcFile.get().string().c_str());
            curl_easy_setopt(req, CURLOPT_NETRC, CURL_NETRC_OPTIONAL);

            if (request.byteRange) {
                /* Ranged chunk of a chunked download. On retry, writtenToSink
                   bytes of this chunk have already been consumed, so resume
                   from there — via the range itself, never via
                   CURLOPT_RESUME_FROM_LARGE, which would add a second,
                   conflicting Range header. */
                auto [first, last] = *request.byteRange;
                curl_easy_setopt(req, CURLOPT_RANGE, fmt("%d-%d", first + writtenToSink, last).c_str());
                /* Concurrent ranged chunks must each get their own TCP
                   connection (and thus their own congestion window) to
                   actually increase throughput; HTTP/2 would multiplex them
                   onto a single connection. */
                curl_easy_setopt(req, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
            } else if (writtenToSink)
                curl_easy_setopt(req, CURLOPT_RESUME_FROM_LARGE, writtenToSink);

            /* Note that the underlying strings get copied by libcurl, so the path -> string conversion is ok:
               > The application does not have to keep the string around after setting this option.
               https://curl.se/libcurl/c/CURLOPT_SSLKEY.html
               https://curl.se/libcurl/c/CURLOPT_SSLCERT.html */

            if (request.tlsCert) {
                curl_easy_setopt(req, CURLOPT_SSLCERTTYPE, "PEM");
                curl_easy_setopt(req, CURLOPT_SSLCERT, request.tlsCert->string().c_str());
            }

            if (request.tlsKey) {
                curl_easy_setopt(req, CURLOPT_SSLKEYTYPE, "PEM");
                curl_easy_setopt(req, CURLOPT_SSLKEY, request.tlsKey->string().c_str());
            }

            curl_easy_setopt(req, CURLOPT_ERRORBUFFER, errbuf);
            errbuf[0] = 0;

            // Set up username/password authentication if provided
            if (request.usernameAuth) {
                curl_easy_setopt(req, CURLOPT_USERNAME, request.usernameAuth->username.c_str());
                if (request.usernameAuth->password) {
                    curl_easy_setopt(req, CURLOPT_PASSWORD, request.usernameAuth->password->c_str());
                }
            }

#if NIX_WITH_AWS_AUTH
            // Set up AWS SigV4 signing if this is an S3 request
            // Note: AWS SigV4 support guaranteed available (curl >= 7.75.0 checked at build time)
            // The username/password (access key ID and secret key) are set via the general
            // usernameAuth mechanism above.
            if (request.awsSigV4Provider) {
                curl_easy_setopt(req, CURLOPT_AWS_SIGV4, request.awsSigV4Provider->c_str());
            }
#endif

            // This seems to be the earliest libcurl callback that signals that the download is happening, so we can
            // call act().
            curl_easy_setopt(req, CURLOPT_RESOLVER_START_FUNCTION, resolverCallbackWrapper);
            curl_easy_setopt(req, CURLOPT_RESOLVER_START_DATA, this);

            result.data.clear();
            result.bodySize = 0;
        }

        void finish(CURLcode code)
        {
            auto finishTime = std::chrono::steady_clock::now();

            auto httpStatus = getHTTPStatus();

            debug(
                "finished %s of '%s'; curl status = %d, HTTP status = %d, body = %d bytes, duration = %.2f s",
                Uncolored(request.noun()),
                request.displayUri(),
                code,
                httpStatus,
                result.bodySize,
                std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - startTime).count() / 1000.0f);

            appendCurrentUrl();

            if (code == CURLE_WRITE_ERROR && result.etag == request.expectedETag) {
                code = CURLE_OK;
                httpStatus = std::to_underlying(HttpStatus::NotModified);
            }

            if (callbackException)
                failEx(callbackException);

            else if (code == CURLE_OK && successfulStatuses.count(httpStatus)) {
                result.cached = (httpStatus == HttpStatus::NotModified);

                // In 2021, GitHub responds to If-None-Match with 304,
                // but omits ETag. We just use the If-None-Match etag
                // since 304 implies they are the same.
                if (httpStatus == HttpStatus::NotModified && result.etag == "")
                    result.etag = request.expectedETag;

                curl_off_t dlSize = 0;
                curl_easy_getinfo(req, CURLINFO_SIZE_DOWNLOAD_T, &dlSize);
                act().progress(dlSize, dlSize);
                maybeNotifyResponse();
                done = true;
                callback(std::move(result));
            }

            else {
                // We treat most errors as transient, but won't retry when hopeless
                Error err = Transient;

                // S3 returns certain retryable errors as HTTP 400/500/503 with XML error codes.
                // These take precedence over the generic HTTP status handling below.
                // Only parse the response body on status codes where S3 XML errors can appear.
                static constexpr std::array<std::string_view, 12> s3RetryableErrors{{
                    "IncompleteBody",       // HTTP 400 - network issue
                    "InternalError",        // HTTP 500 - S3 internal failure
                    "InternalFailure",      // HTTP 500 - alias for InternalError
                    "InternalServerError",  // HTTP 500 - alias for InternalError
                    "RequestExpired",       // HTTP 400 - clock skew / slow upload
                    "RequestTimeout",       // HTTP 400 - stale connection reuse
                    "RequestTimeTooSkewed", // HTTP 403 - clock drift
                    "RequestThrottled",     // HTTP 400 - throttling variant
                    "SlowDown",             // HTTP 503 - throttling
                    "ServiceUnavailable",   // HTTP 503 - temporary unavailability
                    "Throttling",           // HTTP 400 - throttling variant
                    "ThrottledException",   // HTTP 400 - throttling variant
                }};
                // S3 error responses have the form <Error><Code>...</Code>...</Error>.
                // Require the <Error> root to avoid matching unrelated XML with a <Code> element.
                static std::regex s3ErrorCodeRegex("<Error>[^]*<Code>([^<]+)</Code>");
                std::smatch s3Match;
                bool isS3XmlStatus = httpStatus == 400 || httpStatus == 403 || httpStatus == 500 || httpStatus == 503;
                auto s3ErrorCode =
                    (isS3XmlStatus && errorSink && std::regex_search(errorSink->s, s3Match, s3ErrorCodeRegex))
                        ? s3Match[1].str()
                        : "";

                if (std::find(s3RetryableErrors.begin(), s3RetryableErrors.end(), s3ErrorCode)
                    != s3RetryableErrors.end()) {
                    debug("S3 error '%s', will retry", s3ErrorCode);
                } else if (
                    httpStatus == HttpStatus::NotFound || httpStatus == HttpStatus::Gone
                    || code == CURLE_FILE_COULDNT_READ_FILE) {
                    // The file is definitely not there
                    err = NotFound;
                } else if (httpStatus == HttpStatus::Unauthorized || httpStatus == HttpStatus::ProxyAuthRequired) {
                    err = Unauthorized;
                } else if (httpStatus == HttpStatus::Forbidden) {
                    // Don't retry on authentication/authorization failures.
                    // Note: the only reason we treat this differently from 401/407 is S3 returns 403 if a file doesn't
                    // exist and the bucket is unlistable.
                    err = Forbidden;
                } else if (
                    httpStatus >= 400 && httpStatus < 500 && httpStatus != HttpStatus::RequestTimeout
                    && httpStatus != HttpStatus::TooManyRequests) {
                    // Most 4xx errors are client errors and are probably not worth retrying:
                    //   * 408 means the server timed out waiting for us, so we try again
                    err = Misc;
                } else if (
                    httpStatus == HttpStatus::NotImplemented || httpStatus == HttpStatus::HttpVersionNotSupported
                    || httpStatus == HttpStatus::NetworkAuthRequired) {
                    // Let's treat most 5xx (server) errors as transient, except for a handful:
                    //   * 501 not implemented
                    //   * 505 http version not supported
                    //   * 511 we're behind a captive portal
                    err = Misc;
                } else {
// Don't bother retrying on certain cURL errors either

// Allow selecting a subset of enum values
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"
                    switch (code) {
                    case CURLE_FAILED_INIT:
                    case CURLE_URL_MALFORMAT:
                    case CURLE_NOT_BUILT_IN:
                    case CURLE_REMOTE_ACCESS_DENIED:
                    case CURLE_FILE_COULDNT_READ_FILE:
                    case CURLE_FUNCTION_NOT_FOUND:
                    case CURLE_ABORTED_BY_CALLBACK:
                    case CURLE_BAD_FUNCTION_ARGUMENT:
                    case CURLE_INTERFACE_FAILED:
                    case CURLE_UNKNOWN_OPTION:
                    case CURLE_SSL_CACERT_BADFILE:
                    case CURLE_TOO_MANY_REDIRECTS:
                    case CURLE_WRITE_ERROR:
                    case CURLE_UNSUPPORTED_PROTOCOL:
                    case CURLE_BAD_CONTENT_ENCODING:
                        err = Misc;
                        break;
                    default: // Shut up warnings
                        break;
                    }
#pragma GCC diagnostic pop
                }

                attempt++;

                std::optional<std::string> response;
                if (errorSink)
                    response = std::move(errorSink->s);

                /* TODO: Also support per-transfer cancellations. */
                if (code == CURLE_ABORTED_BY_CALLBACK && getInterrupted()) {
                    failInterruptedOrCancelled();
                    return;
                }

                auto exc = httpStatus != 0
                               ? FileTransferError(
                                     err,
                                     std::move(response),
                                     "unable to %s '%s': HTTP error %d%s",
                                     Uncolored(request.verb()),
                                     request.displayUri(),
                                     httpStatus,
                                     code == CURLE_OK ? "" : fmt(" (curl error: %s)", curl_easy_strerror(code)))
                               : FileTransferError(
                                     err,
                                     std::move(response),
                                     "unable to %s '%s': %s (%d) %s",
                                     Uncolored(request.verb()),
                                     request.displayUri(),
                                     curl_easy_strerror(code),
                                     code,
                                     errbuf);

                maybeRetry(err, httpStatus, std::move(exc));
            }
        }

        /* If this is a transient error, then maybe retry the download
           after a while. If we're writing to a sink, we can only retry
           if the server supports ranged requests. */
        void maybeRetry(FileTransfer::Error err, long httpStatus, FileTransferError && exc)
        {
            // Resolve effective retry config (request overrides > global settings)
            auto effAttempts = request.retryAttempts.value_or(fileTransfer.settings.tries);
            auto effBaseMs = request.retryDelayMs.value_or(fileTransfer.settings.retryDelayMs);
            auto effRateLimitMs =
                request.retryDelayRateLimitedMs.value_or(fileTransfer.settings.retryDelayRateLimitedMs);
            auto effMaxMs = request.retryMaxDelayMs.value_or(fileTransfer.settings.retryMaxDelayMs);

            // Pick base delay by error class: 429/503 indicate the server is
            // rate-limiting or overloaded, so use the longer rate-limit delay.
            uint32_t baseMs =
                (httpStatus == HttpStatus::TooManyRequests || httpStatus == HttpStatus::ServiceUnavailable)
                    ? effRateLimitMs
                    : effBaseMs;

            auto canRetry = [&] {
                if (err != Transient)
                    return false;
                if (attempt >= effAttempts)
                    return false;
                // If we've already streamed bytes to the callback, we can only
                // resume via a Range request. That requires the server to accept
                // byte ranges AND the response to be uncompressed (the Range
                // applies to the encoded stream, but the sink saw decoded bytes).
                if (request.dataCallback && writtenToSink != 0)
                    return acceptRanges && !hasContentEncoding;
                return true;
            }();

            if (!canRetry) {
                fail(std::move(exc));
                return;
            }

            auto delay = computeRetryDelayMs(
                {
                    .attempt = attempt,
                    .baseMs = baseMs,
                    .ceilMs = effMaxMs,
                    .retryAfterMs = std::exchange(retryAfterMs, std::nullopt),
                    .jitter = fileTransfer.settings.retryJitter,
                },
                fileTransfer.mt19937);

            if (writtenToSink) {
                warn(
                    "%s; retrying from offset %d in %d ms (attempt %d/%d)",
                    exc.message(),
                    writtenToSink,
                    delay.count(),
                    attempt,
                    effAttempts);
            } else {
                warn("%s; retrying in %d ms (attempt %d/%d)", exc.message(), delay.count(), attempt, effAttempts);
            }

            errorSink.reset();
            embargo = std::chrono::steady_clock::now() + delay;
            try {
                fileTransfer.enqueueItem(ref{shared_from_this()});
            } catch (const nix::Error & e) {
                // If enqueue fails (e.g., during shutdown), fail the transfer properly
                // instead of letting the exception propagate, which would leave done=false
                // and cause the destructor to attempt a second callback invocation
                fail(std::move(exc));
            }
        }
    };

    struct State
    {
        struct EmbargoComparator
        {
            bool operator()(const ref<TransferItem> & i1, const ref<TransferItem> & i2)
            {
                return i1->embargo > i2->embargo;
            }
        };

        std::priority_queue<ref<TransferItem>, std::vector<ref<TransferItem>>, EmbargoComparator> incoming;
        std::vector<std::weak_ptr<Item>> unpause;
    private:
        bool quitting = false;
    public:
        bool work = false;

        /**
         * Stop accepting requests and drain the queue. The drained items
         * are RETURNED rather than destroyed here: destroying a queued
         * item runs its failure callback, and callbacks must never run
         * under the state lock (a chunked-download callback may re-enter
         * enqueueItem()).
         */
        [[nodiscard]] std::vector<ref<TransferItem>> quit()
        {
            quitting = true;
            /* We will not be processing any more incoming requests */
            std::vector<ref<TransferItem>> dropped;
            while (!incoming.empty()) {
                dropped.push_back(incoming.top());
                incoming.pop();
            }
            unpause.clear();
            return dropped;
        }

        bool isQuitting()
        {
            return quitting;
        }
    };

    Sync<State> state_;

    /**
     * Number of transfers currently added to the curl multi handle.
     * Maintained by the worker thread; read by chunked downloads to
     * estimate idle connection-pool slots.
     */
    std::atomic<size_t> activeHandles{0};

    std::thread workerThread;

    const size_t maxQueueSize;

    curlFileTransfer(const FileTransferSettings & settings)
        : settings(settings)
        , mt19937(rd())
        , maxQueueSize([&]() -> std::size_t {
            if (settings.httpConnections.get())
                return settings.httpConnections.get() * 5;
            /* Zero means unlimited. See https://curl.se/libcurl/c/CURLMOPT_MAX_TOTAL_CONNECTIONS.html. */
            return std::numeric_limits<std::size_t>::max();
        }())
    {
        static std::once_flag globalInit;
        std::call_once(globalInit, curl_global_init, CURL_GLOBAL_ALL);

        curlm = curlMulti(curl_multi_init());

        curl_multi_setopt(curlm.get(), CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
        curl_multi_setopt(curlm.get(), CURLMOPT_MAX_TOTAL_CONNECTIONS, settings.httpConnections.get());

        workerThread = std::thread([&]() { workerThreadEntry(); });
    }

    ~curlFileTransfer() override;

    void stopWorkerThread()
    {
        std::vector<ref<TransferItem>> dropped;
        {
            /* Signal the worker thread to exit. */
            auto state(state_.lock());
            dropped = state->quit();
            wakeupMulti(*state);
        }
        /* Destroyed outside the lock: this fails the items' callbacks. */
    }

    void wakeupMulti(State & state)
    {
        state.work = true;
        if (auto ec = ::curl_multi_wakeup(curlm.get()))
            throw curlMultiError(ec);
    }

    void workerThreadMain()
    {
/* Cause this thread to be notified on SIGINT. */
#ifndef _WIN32 // TODO need graceful async exit support on Windows?
        auto callback = createInterruptCallback([&]() { stopWorkerThread(); });
#endif

#ifdef __linux__
        try {
            tryUnshareFilesystem();
        } catch (nix::Error & e) {
            e.addTrace({}, "in download thread");
            throw;
        }
#endif

        std::map<CURL *, std::shared_ptr<TransferItem>> items;

        bool quit = false;

        std::chrono::steady_clock::time_point nextWakeup;

        while (!quit) {
            checkInterrupt();

            /* Let curl do its thing. */
            int running;
            CURLMcode mc = curl_multi_perform(curlm.get(), &running);
            if (mc != CURLM_OK)
                throw nix::Error("unexpected error from curl_multi_perform(): %s", curl_multi_strerror(mc));

            /* Set the promises of any finished requests. */
            CURLMsg * msg;
            int left;
            while ((msg = curl_multi_info_read(curlm.get(), &left))) {
                if (msg->msg == CURLMSG_DONE) {
                    auto i = items.find(msg->easy_handle);
                    assert(i != items.end());
                    /* Decrement before finish(): its callback may compute
                       idle slots for a chunked download's fan-out, and this
                       handle no longer occupies a slot. */
                    activeHandles--;
                    i->second->finish(msg->data.result);
                    curl_multi_remove_handle(curlm.get(), i->second->req);
                    i->second->active = false;
                    items.erase(i);
                }
            }

            nextWakeup = std::chrono::steady_clock::time_point();

            std::vector<std::shared_ptr<TransferItem>> incoming;
            std::vector<std::weak_ptr<Item>> unpause;
            auto now = std::chrono::steady_clock::now();
            bool haveWork;

            {
                auto state(state_.lock());
                while (!state->incoming.empty()) {
                    /* Limit the number of active curl handles, since curl doesn't scale well. */
                    if (items.size() + incoming.size() >= maxQueueSize) {
                        auto t = now + std::chrono::milliseconds(100);
                        if (nextWakeup == std::chrono::steady_clock::time_point() || t < nextWakeup)
                            nextWakeup = t;
                        break;
                    }
                    auto item = state->incoming.top();
                    if (item->embargo <= now) {
                        incoming.push_back(item);
                        state->incoming.pop();
                    } else {
                        if (nextWakeup == std::chrono::steady_clock::time_point() || item->embargo < nextWakeup)
                            nextWakeup = item->embargo;
                        break;
                    }
                }
                unpause = std::exchange(state->unpause, {});
                quit = state->isQuitting();
                haveWork = std::exchange(state->work, false);
            }

            for (auto & item : incoming) {
                debug("starting %s of '%s'", Uncolored(item->request.noun()), item->request.displayUri());
                item->init();
                curl_multi_add_handle(curlm.get(), item->req);
                item->active = true;
                activeHandles++;
                items[item->req] = item;
            }

            if (quit)
                break;

            /* NOTE: Unpausing may invoke callbacks to flush all buffers. */
            for (auto & item : unpause) {
                /* The transfer might have completed (failed) between it getting
                   enqueued for unpause and by the time the worker thread picked
                   it up. */
                auto ptr = item.lock();
                if (!ptr)
                    continue;
                static_cast<TransferItem &>(*ptr).unpause();
            }

            /* Wait for activity, including wakeup events. */
            long maxSleepTimeMs = items.empty() ? 10000 : 100;
            auto sleepTimeMs = nextWakeup != std::chrono::steady_clock::time_point()
                                   ? std::max(
                                         0,
                                         (int) std::chrono::duration_cast<std::chrono::milliseconds>(
                                             nextWakeup - std::chrono::steady_clock::now())
                                             .count())
                                   : maxSleepTimeMs;

            /* Since https://github.com/curl/curl/commit/2a2104f3cff44bb28bb570a093be52bbeeed8f23 (8.21),
               curl_multi_perform seems to swallow queued up events ¯\_(ツ)_/¯. */
            if (haveWork)
                sleepTimeMs = 0;

            int numfds = 0;
            mc = curl_multi_poll(curlm.get(), nullptr, 0, sleepTimeMs, &numfds);
            if (mc != CURLM_OK)
                throw curlMultiError(mc);
        }

        debug("download thread shutting down");
    }

    void workerThreadEntry()
    {
        // Unwinding or because someone called `quit`.
        bool normalExit = true;
        try {
            workerThreadMain();
        } catch (nix::Interrupted & e) {
            normalExit = false;
        } catch (std::exception & e) {
            printError("unexpected error in download thread: %s", e.what());
            normalExit = false;
        }

        if (!normalExit) {
            std::vector<ref<TransferItem>> dropped;
            {
                auto state(state_.lock());
                dropped = state->quit();
            }
            /* Destroyed outside the lock: this fails the items' callbacks. */
        }
    }

    ItemHandle enqueueItem(ref<TransferItem> item)
    {
        if (item->request.data && item->request.uri.scheme() != "http" && item->request.uri.scheme() != "https"
            && item->request.uri.scheme() != "s3")
            throw nix::Error("uploading to '%s' is not supported", item->request.displayUri());

        {
            auto state(state_.lock());
            if (state->isQuitting())
                throw nix::Error("cannot enqueue download request because the download thread is shutting down");
            state->incoming.push(item);
            item->enqueued = true; /* Now any exceptions should be reported via the callback. */
            wakeupMulti(*state);
        }

        return ItemHandle(item.get_ptr());
    }

    ItemHandle enqueueFileTransfer(const FileTransferRequest & request, Callback<FileTransferResult> callback) override
    {
        /* Handle s3:// URIs by converting to HTTPS and optionally adding auth */
        if (request.uri.scheme() == "s3") {
            auto modifiedRequest = request;
            modifiedRequest.setupForS3();
            return enqueueItem(make_ref<TransferItem>(*this, std::move(modifiedRequest), std::move(callback)));
        }

        return enqueueItem(make_ref<TransferItem>(*this, request, std::move(callback)));
    }

    void unpauseTransfer(std::weak_ptr<Item> item)
    {
        auto state(state_.lock());
        state->unpause.push_back(std::move(item));
        wakeupMulti(*state);
    }

    void unpauseTransfer(ItemHandle handle) override
    {
        /* The transfer might have completed (more likely failed) when we want
           to wake it up. That's why we must use a weak_ptr throughout. */
        unpauseTransfer(handle.item);
    }

    /**
     * Shared state and callbacks of one chunked (parallel ranged)
     * download. The sink-feeding drain loop runs on the caller's thread
     * (see downloadChunked()); all callbacks run on the transfer thread.
     *
     * Kept alive by the per-chunk callbacks, so it outlives the caller
     * when the caller unwinds with chunks still in flight.
     */
    struct ChunkedDownload : std::enable_shared_from_this<ChunkedDownload>
    {
        curlFileTransfer & ft;
        const FileTransferRequest baseRequest;
        const ChunkPlan plan;

        struct State
        {
            /** The caller is gone or the transfer failed; drop further data. */
            bool quit = false;

            /** First failure; rethrown by the drain loop. */
            std::exception_ptr exc;

            /** In-order bytes ready for the sink. */
            std::string drainBuf;

            /** Whether the head chunk paused because drainBuf ran full. */
            bool headPaused = false;

            /** Lowest incomplete chunk; its bytes stream straight to drainBuf. */
            uint64_t headChunk = 0;

            /** Out-of-order buffers of chunks ahead of the head. */
            std::map<uint64_t, std::string> reorder;

            /** Finished chunks the head hasn't been advanced past yet. */
            std::set<uint64_t> completed;

            /** Handles of in-flight chunks, for unpausing the head. */
            std::map<uint64_t, std::weak_ptr<Item>> handles;

            /** Bytes received per in-flight or completed chunk. */
            std::map<uint64_t, uint64_t> received;

            /** Whether the server ignored the Range probe (plain download). */
            bool fallback = false;

            uint64_t nextToDispatch = 0;
            uint64_t chunksDone = 0;
            size_t inFlight = 0;

            /** Effective chunk count; drops to 1 on fallback to a plain download. */
            uint64_t chunksTotal;

            /** Whether chunk 0's 206-vs-200 verdict has arrived. */
            bool verdict = false;

            /** ETag of chunk 0's response; later chunks send If-Match. */
            std::string etag;

            /** Result of chunk 0, for the resultCallback. */
            std::optional<FileTransferResult> firstResult;

            std::condition_variable avail;
        };

        Sync<State> state_;

        ChunkedDownload(curlFileTransfer & ft, FileTransferRequest && baseRequest, const ChunkPlan & plan)
            : ft(ft)
            , baseRequest(std::move(baseRequest))
            , plan(plan)
        {
            state_.lock()->chunksTotal = plan.numChunks();
        }

        /**
         * Advance the head past finished chunks, splicing their buffered
         * bytes into drainBuf in order. Must be called with the state lock
         * held.
         */
        void promote(State & state)
        {
            while (state.completed.count(state.headChunk)) {
                state.completed.erase(state.headChunk);
                state.headChunk++;
                /* Any pause belonged to the chunk we just advanced past. */
                state.headPaused = false;
                if (auto it = state.reorder.find(state.headChunk); it != state.reorder.end()) {
                    state.drainBuf.append(it->second);
                    state.reorder.erase(it);
                }
            }
        }

        /**
         * Top up the in-flight window with however many idle connection
         * slots the pool has right now. Must be called with the state lock
         * held.
         */
        void topUp(State & state)
        {
            if (state.quit || !state.verdict)
                return;
            auto n = chunkDispatchCount(
                state.chunksTotal - state.nextToDispatch,
                state.inFlight,
                ft.activeHandles.load(),
                ft.settings.httpConnections);
            try {
                while (n--)
                    dispatch(state.nextToDispatch++, state);
            } catch (...) {
                /* enqueueItem() can throw during shutdown. */
                if (!state.exc)
                    state.exc = std::current_exception();
                state.quit = true;
            }
        }

        /**
         * Enqueue chunk `i`. Must be called with the state lock held.
         */
        void dispatch(uint64_t i, State & state)
        {
            auto chunkReq = baseRequest;
            auto range = plan.range(i);
            chunkReq.byteRange = {{range.first, range.last}};
            if (i == 0)
                /* Chunk 0 doubles as the range-support probe. */
                chunkReq.responseCallback =
                    [self = shared_from_this()](
                        long httpStatus, std::optional<uint64_t> rangeTotal, const std::string & etag) {
                        self->onResponse(httpStatus, rangeTotal, etag);
                    };
            else if (!state.etag.empty() && !state.etag.starts_with("W/"))
                /* Guard against the object changing between chunks. (Weak
                   etags never match under If-Match's strong comparison, so
                   sending one would always fail.) */
                chunkReq.headers.emplace_back("If-Match", state.etag);
            chunkReq.dataCallback = [self = shared_from_this(), i](std::string_view data) {
                return self->onData(i, data);
            };
            state.inFlight++;
            auto handle = ft.enqueueItem(
                make_ref<TransferItem>(
                    ft,
                    std::move(chunkReq),
                    Callback<FileTransferResult>([self = shared_from_this(), i](std::future<FileTransferResult> fut) {
                        self->onDone(i, std::move(fut));
                    })));
            state.handles[i] = handle.item;
        }

        /**
         * Number of bytes chunk `i` is allowed to deliver. Must be called
         * with the state lock held.
         */
        uint64_t expectedChunkBytes(const State & state, uint64_t i) const
        {
            /* After the 200 fallback, chunk 0's response is the whole file. */
            if (state.fallback && i == 0)
                return plan.fileSize;
            auto range = plan.range(i);
            return range.last - range.first + 1;
        }

        PauseTransfer onData(uint64_t i, std::string_view data)
        {
            auto state(state_.lock());
            if (state->quit)
                return PauseTransfer::No;

            /* Bound what a chunk may deliver. This keeps the reorder
               buffers bounded by the chunk size even against a server
               that answers a ranged request with more data than asked
               for (e.g. a 200 with the full body on a non-probe chunk),
               and turns such responses into a hard error instead of a
               corrupt stream. */
            auto & received = state->received[i];
            received += data.size();
            if (received > expectedChunkBytes(*state, i)) {
                if (!state->exc)
                    state->exc = std::make_exception_ptr(
                        nix::Error(
                            "chunked download of '%s' failed: server sent more data than requested for bytes %d-%d",
                            baseRequest.displayUri(),
                            plan.range(i).first,
                            plan.range(i).last));
                state->quit = true;
                state->avail.notify_one();
                return PauseTransfer::No;
            }

            if (i == state->headChunk) {
                state->drainBuf.append(data);
                state->avail.notify_one();
                if (state->drainBuf.size() > ft.settings.downloadBufferSize) {
                    state->headPaused = true;
                    return PauseTransfer::Yes;
                }
            } else
                /* Out-of-order data, bounded by the chunk size above; the
                   window bounds how many chunks are in flight. */
                state->reorder[i].append(data);
            return PauseTransfer::No;
        }

        void onResponse(long httpStatus, std::optional<uint64_t> rangeTotal, const std::string & etag)
        {
            auto state(state_.lock());
            if (state->verdict || state->quit)
                return;
            state->verdict = true;
            if (httpStatus == std::to_underlying(HttpStatus::PartialContent)) {
                if (rangeTotal && *rangeTotal != plan.fileSize) {
                    state->exc = std::make_exception_ptr(
                        nix::Error(
                            "chunked download of '%s' failed: expected a file of %d bytes, but the server reports %d bytes",
                            baseRequest.displayUri(),
                            plan.fileSize,
                            *rangeTotal));
                    state->quit = true;
                } else {
                    state->etag = etag;
                    topUp(*state);
                }
            } else {
                /* The server ignored the Range request and is sending the
                   whole body over this connection; consume it as a plain
                   download. (TransferItem has already cleared byteRange so
                   retries resume linearly.) */
                debug(
                    "server of '%s' does not support byte ranges, falling back to a regular download",
                    baseRequest.displayUri());
                state->fallback = true;
                state->chunksTotal = 1;
            }
            state->avail.notify_one();
        }

        void onDone(uint64_t i, std::future<FileTransferResult> fut)
        {
            auto state(state_.lock());
            state->inFlight--;
            state->chunksDone++;
            state->handles.erase(i);
            try {
                auto res = fut.get();
                /* received[] accumulates across retry-resumes of the same
                   chunk; res.bodySize would only cover the last attempt. */
                if (state->received[i] != expectedChunkBytes(*state, i))
                    throw nix::Error(
                        "chunked download of '%s' failed: server sent %d bytes for bytes %d-%d",
                        baseRequest.displayUri(),
                        state->received[i],
                        plan.range(i).first,
                        plan.range(i).last);
                if (i == 0)
                    state->firstResult = std::move(res);
                else if (!state->etag.empty() && !res.etag.empty() && res.etag != state->etag)
                    /* Server ignored (or couldn't evaluate) If-Match, but the
                       ETag shows the object changed while we were fetching
                       ranges of it. */
                    throw nix::Error(
                        "chunked download of '%s' failed: file changed on the server while it was being downloaded",
                        baseRequest.displayUri());
                state->completed.insert(i);
                promote(*state);
                topUp(*state);
            } catch (...) {
                if (!state->exc)
                    state->exc = std::current_exception();
                state->quit = true;
            }
            state->avail.notify_one();
        }
    };

    bool tryChunkedDownload(
        FileTransferRequest & request, Sink & sink, std::function<void(FileTransferResult)> & resultCallback) override
    {
        if (!settings.parallelDownloadChunkSize)
            return false;
        if (request.method != HttpMethod::Get || request.data)
            return false;
        auto chunkSize = std::max<uint64_t>(settings.parallelDownloadChunkSize, 1024 * 1024);
        if (chunkSize != settings.parallelDownloadChunkSize) {
            static std::atomic<bool> haveWarned = false;
            warnOnce(
                haveWarned,
                "rounding 'parallel-download-chunk-size' (%d) up to 1 MiB",
                settings.parallelDownloadChunkSize.get());
        }
        if (!request.expectedSize || *request.expectedSize <= chunkSize)
            return false;
        auto fileSize = *request.expectedSize;

        if (request.uri.scheme() == "s3")
            request.setupForS3();

        downloadChunked(
            std::make_shared<ChunkedDownload>(
                *this, std::move(request), ChunkPlan{.fileSize = fileSize, .chunkSize = chunkSize}),
            sink,
            resultCallback);
        return true;
    }

    /**
     * Drain loop of a chunked download: feed in-order bytes to the sink on
     * the calling thread, unpause the head chunk when the buffer empties,
     * finish when every chunk is done.
     */
    void downloadChunked(
        std::shared_ptr<ChunkedDownload> dl, Sink & sink, std::function<void(FileTransferResult)> & resultCallback)
    {
        /* On unwind (exception in the sink, interrupt), tell the chunk
           callbacks to drop further data, and unpause a paused head chunk —
           nothing would ever unpause it otherwise, permanently occupying a
           connection slot. In-flight chunks then drain to completion in the
           background (their data is discarded), holding `dl` alive. */
        Finally finally([&]() {
            std::optional<std::weak_ptr<Item>> toUnpause;
            {
                auto state(dl->state_.lock());
                state->quit = true;
                if (state->headPaused) {
                    state->headPaused = false;
                    if (auto it = state->handles.find(state->headChunk); it != state->handles.end())
                        toUnpause = it->second;
                }
            }
            if (toUnpause)
                unpauseTransfer(*toUnpause);
        });

        {
            auto state(dl->state_.lock());
            dl->dispatch(state->nextToDispatch++, *state);
        }

        while (true) {
            checkInterrupt();

            std::string chunk;

            {
                auto state(dl->state_.lock());

                while (state->drainBuf.empty()) {
                    if (state->exc)
                        std::rethrow_exception(state->exc);

                    if (state->chunksDone >= state->chunksTotal) {
                        if (resultCallback && state->firstResult) {
                            /* Present the result as one logical download. */
                            state->firstResult->bodySize = dl->plan.fileSize;
                            resultCallback(std::move(*state->firstResult));
                        }
                        return;
                    }

                    if (state->headPaused) {
                        state->headPaused = false;
                        if (auto it = state->handles.find(state->headChunk); it != state->handles.end())
                            unpauseTransfer(it->second);
                    }

                    state.wait(state->avail);
                }

                chunk = std::move(state->drainBuf);
                state->drainBuf.clear();
            }

            /* Flush to the sink without holding the lock, so the transfer
               thread isn't blocked by an expensive sink. */
            sink(chunk);
        }
    }
};

curlFileTransfer::~curlFileTransfer()
{
    try {
        stopWorkerThread();
    } catch (...) {
        ignoreExceptionInDestructor();
    }
    workerThread.join();
}

ref<curlFileTransfer> makeCurlFileTransfer(const FileTransferSettings & settings = fileTransferSettings)
{
    return make_ref<curlFileTransfer>(settings);
}

static auto * const _fileTransfer = new Sync<std::shared_ptr<curlFileTransfer>>;

ref<FileTransfer> getFileTransfer()
{
    auto fileTransfer(_fileTransfer->lock());

    if (!*fileTransfer || (*fileTransfer)->state_.lock()->isQuitting())
        *fileTransfer = makeCurlFileTransfer().get_ptr();

    return ref<FileTransfer>(*fileTransfer);
}

ref<FileTransfer> makeFileTransfer(const FileTransferSettings & settings)
{
    return makeCurlFileTransfer(settings);
}

std::string FileTransferRequest::displayUri() const
{
    try {
        auto parsed = uri.parsed();
        if (parsed.authority && parsed.authority->user) {
            parsed.authority->user.reset();
            parsed.authority->password.reset();
            return parsed.to_string();
        }
    } catch (BadURL &) {
    }
    return uri.to_string();
}

void FileTransferRequest::setupForS3()
{
    auto parsedS3 = ParsedS3URL::parse(uri.parsed());
    // Update the request URI to use HTTPS (works without AWS SDK)
    uri = parsedS3.toHttpsUrl();

#if NIX_WITH_AWS_AUTH
    // Auth-specific code only compiled when AWS support is available
    awsSigV4Provider = "aws:amz:" + parsedS3.region.value_or("us-east-1") + ":s3";

    // check if the request already has pre-resolved credentials
    std::optional<std::string> sessionToken;
    if (usernameAuth) {
        debug("Using pre-resolved AWS credentials from parent process");
        sessionToken = preResolvedAwsSessionToken;
    } else if (auto creds = getAwsCredentialsProvider()->maybeGetCredentials(parsedS3)) {
        usernameAuth = UsernameAuth{
            .username = creds->accessKeyId,
            .password = creds->secretAccessKey,
        };
        sessionToken = creds->sessionToken;
    }
    if (sessionToken)
        headers.emplace_back("x-amz-security-token", *sessionToken);
#else
    // When built without AWS support, just try as public bucket
    debug("S3 request without authentication (built without AWS support)");
#endif
}

std::future<FileTransferResult> FileTransfer::enqueueFileTransfer(const FileTransferRequest & request)
{
    auto promise = std::make_shared<std::promise<FileTransferResult>>();
    enqueueFileTransfer(request, {[promise](std::future<FileTransferResult> fut) {
                            try {
                                promise->set_value(fut.get());
                            } catch (...) {
                                promise->set_exception(std::current_exception());
                            }
                        }});
    return promise->get_future();
}

FileTransferResult FileTransfer::download(const FileTransferRequest & request)
{
    return enqueueFileTransfer(request).get();
}

FileTransferResult FileTransfer::upload(const FileTransferRequest & request)
{
    /* Note: this method is the same as download, but helps in readability */
    return enqueueFileTransfer(request).get();
}

FileTransferResult FileTransfer::deleteResource(const FileTransferRequest & request)
{
    return enqueueFileTransfer(request).get();
}

void FileTransfer::download(
    FileTransferRequest && request, Sink & sink, std::function<void(FileTransferResult)> resultCallback)
{
    if (tryChunkedDownload(request, sink, resultCallback))
        return;

    /* Note: we can't call 'sink' via request.dataCallback, because
       that would cause the sink to execute on the fileTransfer
       thread. If 'sink' is a coroutine, this will fail. Also, if the
       sink is expensive (e.g. one that does decompression and writing
       to the Nix store), it would stall the download thread too much.
       Therefore we use a buffer to communicate data between the
       download thread and the calling thread. */

    struct State
    {
        bool quit = false;
        bool paused = false;
        std::exception_ptr exc;
        std::string data;
        std::condition_variable avail;
    };

    auto _state = std::make_shared<Sync<State>>();

    /* In case of an exception, wake up the download thread. FIXME:
       abort the download request. */
    Finally finally([&]() {
        auto state(_state->lock());
        state->quit = true;
    });

    request.dataCallback = [_state, uri = request.displayUri()](std::string_view data) -> PauseTransfer {
        auto state(_state->lock());

        if (state->quit)
            return PauseTransfer::No;

        /* Append data to the buffer and wake up the calling
           thread. */
        state->data.append(data);
        state->avail.notify_one();

        if (state->data.size() <= fileTransferSettings.downloadBufferSize)
            return PauseTransfer::No;

        /* dataCallback gets called multiple times by an intermediate sink. Only
           issue the debug message the first time around. */
        if (!state->paused)
            debug(
                "pausing transfer for '%s': download buffer is full (%d > %d)",
                uri,
                state->data.size(),
                fileTransferSettings.downloadBufferSize);

        state->paused = true;

        /* Technically the buffer might become larger than
           downloadBufferSize, but with sinks there's no way to avoid
           consuming data. */
        return PauseTransfer::Yes;
    };

    auto handle = enqueueFileTransfer(
        request, {[_state, resultCallback{std::move(resultCallback)}](std::future<FileTransferResult> fut) {
            auto state(_state->lock());
            state->quit = true;
            try {
                auto res = fut.get();
                if (resultCallback)
                    resultCallback(std::move(res));
            } catch (...) {
                state->exc = std::current_exception();
            }
            state->avail.notify_one();
        }});

    while (true) {
        checkInterrupt();

        std::string chunk;

        /* Grab data if available, otherwise wait for the download
           thread to wake us up. */
        {
            auto state(_state->lock());

            if (state->data.empty()) {

                if (state->quit) {
                    if (state->exc)
                        std::rethrow_exception(state->exc);
                    return;
                }

                if (state->paused) {
                    unpauseTransfer(handle);
                    state->paused = false;
                }
                state.wait(state->avail);

                if (state->data.empty())
                    continue;
            }

            chunk = std::move(state->data);
            /* Reset state->data after the move, since we check data.empty() */
            state->data = "";
        }

        /* Flush the data to the sink and wake up the download thread
           if it's blocked on a full buffer. We don't hold the state
           lock while doing this to prevent blocking the download
           thread if sink() takes a long time. */
        sink(chunk);
    }
}

void FileTransferError::anchor() {}

template<typename... Args>
FileTransferError::FileTransferError(
    FileTransfer::Error error, std::optional<std::string> response, const Args &... args)
    : CloneableError(args...)
    , error(error)
    , response(response)
{
    const auto hf = HintFmt(args...);
    // FIXME: Due to https://github.com/NixOS/nix/issues/3841 we don't know how
    // to print different messages for different verbosity levels. For now
    // we add some heuristics for detecting when we want to show the response.
    if (response && (response->size() < 1024 || response->find("<html>") != std::string::npos))
        err.msg = HintFmt("%1%\n\nresponse body:\n\n%2%", Uncolored(hf.str()), chomp(*response));
    else
        err.msg = hf;
}

} // namespace nix
