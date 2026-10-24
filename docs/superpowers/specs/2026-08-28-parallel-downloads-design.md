# Parallel chunked downloads from substituters — design

Date: 2026-08-28
Status: approved by user (brainstorming session)

## Motivation

In regions far from cache.nixos.org's edge, single-connection throughput is
often low (a few MB/s) while aggregate throughput over many connections is
fine. Substitution already parallelizes *across* store paths, so many small
files download fast — but one large NAR left at the end of a build is stuck
on a single connection and can dominate wall time even on a gigabit link.
Measured on cache.nixos.org (Fastly→S3): `Accept-Ranges: bytes`, HTTP 206
honored; 8-way ranged fetch of a 143 MB NAR was ~1.5× faster than a single
connection on a well-connected EU link, with far larger gains expected on
throttled long-haul links.

## Goal

Allow a single large file to be downloaded over multiple HTTP connections
using `Range` requests, reusing the existing `http-connections` pool.
Default off. Servers without range support degrade gracefully to a normal
sequential download. Small files are unaffected.

## Non-goals

- No change to buffered (non-sink) downloads (`.narinfo`, registries, etc.).
- No per-host capability caching.
- No spill-to-disk reassembly.
- No new parallelism across paths (that exists via `max-substitution-jobs`).

## Configuration

Exactly one new setting, in `FileTransferSettings`
(`src/libstore/include/nix/store/filetransfer.hh`):

- `parallel-download-chunk-size` (size, default `0`)
  - `0`: feature disabled; all downloads take the existing code path,
    byte-for-byte identical behavior.
  - `> 0`: any eligible download with known size strictly greater than this
    value is fetched as a sequence of `Range` chunks of this size (last chunk
    truncated). Values below 1 MiB are clamped to 1 MiB with a warning.

The eligibility threshold is the chunk size itself: a file ≤ one chunk is a
single-range download, i.e. effectively sequential. No separate min-size
option.

The existing `http-connections` setting (default 25) remains the sole bound
on total connections; chunks compete for the same pool as everything else.

Documented memory formula (in the setting description): worst-case
reassembly memory ≈ `(http-connections − 1) × parallel-download-chunk-size`.
Users trade memory against request granularity by tuning chunk size (e.g.
16M chunks ≈ ¼ the memory of 64M at identical parallelism).

## Architecture

New component: a **chunked download coordinator** in
`src/libstore/filetransfer.cc`, between `FileTransfer::download(request,
sink, cb)` and the existing `TransferItem` machinery. `TransferItem` stays
single-stream; a chunk is an ordinary `TransferItem` with a `Range` header.

### Eligibility (all must hold)

- `fileTransferSettings.parallelDownloadChunkSize > 0`
- the size hint (`ChunkedDownloadHint::expectedSize`) is absent or `> chunkSize`
- method is GET, no upload data
- sink-based path (`download(request, sink)`), not `enqueueFileTransfer`

Otherwise: existing sequential path, untouched.

`FileTransferRequest` gains `std::optional<ChunkedDownloadHint> chunked`.
Only `BinaryCacheStore::narFromPath` sets it today, with
`expectedSize` from `NarInfo::fileSize` (absent when the cache omits
`FileSize`).

`FileSize` is an untrusted hint: cache.nixos.org has been observed serving
NARs whose size and hash differ from the narinfo's `FileSize`/`FileHash`
(e.g. `39lyf5g2iz4jd2r6q4g5whk9vw09zglj.narinfo`: `FileSize: 9064108`,
served 9064101 bytes). The hint only decides eligibility (skip chunking
small files); a wrong hint at worst costs parallelism. The chunk plan always
comes from the server: chunk 0 is a size probe and the remaining chunks are
planned from the `Content-Range` total of its response. Integrity rests on
the `NarHash`/`NarSize` check of the decompressed NAR when it is added to
the store; `FileHash` is never checked.

### Chunk plan and adaptive window

- Plan: `ceil(size / chunkSize)` contiguous ranges, dispatched strictly in
  ascending offset order.
- Chunk 0 is dispatched alone (it doubles as the range-support probe).
- On every chunk completion (and after chunk 0's verdict), the coordinator
  tops up the in-flight window to `max(1, http-connections − others)`,
  where `others` is the number of active transfer handles that are NOT this
  download's own chunks (own in-flight chunks must not count as pool
  pressure against themselves, or the window would decay toward half the
  pool). Active handles come from an atomic counter maintained by the curl
  worker thread. Pool busy → window 1 ≈ sequential (one 206 per chunk over
  a reused connection, one RTT of overhead per chunk). Pool drains →
  remaining chunks fan out within seconds (at most one chunk boundary
  away). This is how a large NAR that *started* while 16 goals were running
  picks up the freed slots as its siblings finish.

### Reassembly and memory

- The **head chunk** (lowest incomplete offset) streams byte-by-byte to the
  caller's existing drain loop → sink, exactly like today's sequential
  download, including the existing `download-buffer-size` pause backpressure
  when the sink is slow.
- **Non-head chunks** buffer their bytes in a per-chunk buffer until they
  become head; a chunk can never buffer more than its own size.
- No artificial reassembly budget: memory is inherently bounded by roughly
  `http-connections × chunkSize`. A budget below that would cap speedup at
  ≈ `1 + budget/chunkSize`, defeating the feature.
- Every chunk's delivered byte count is tracked: a chunk delivering more
  bytes than its range (e.g. a misbehaving server answering 200 with the
  full body to a non-probe chunk) is a hard error, which also keeps the
  reorder buffers bounded against malicious servers.
- Bytes handed to the sink are gone from memory immediately (the NAR sink
  decompresses and unpacks to disk while streaming).

### Data flow

```
curl (N easy handles, Range: bytes=a-b, Accept-Encoding disabled)
  → per-chunk dataCallback
      head chunk    → shared drain buffer (existing pause protocol)
      non-head      → per-chunk reorder buffer (≤ chunkSize each)
  → caller thread drain loop (in-order)
  → decompression sink → NAR parser → store
```

`Accept-Encoding` (curl transparent compression) is disabled on ranged
requests — ranges apply to the encoded stream; the existing resume-retry
code already documents this constraint. NAR compression is explicit
(`Compression:` in the narinfo) and handled by Nix's own decompression sink,
so nothing is lost.

## Error handling

- **Server ignores `Range`** (200 instead of 206 on chunk 0): the response
  is the full body — keep streaming it as a normal sequential download,
  dispatch no further chunks. No error, no extra requests, no wasted bytes.
  Its length is whatever the server sends; the size hint is not enforced.
- **No usable size**: a 206 without a `Content-Range` total (`bytes a-b/*`)
  abandons the probe and refetches the file as a plain download.
- **Size hint mismatch**: a `Content-Range` total differing from the hint is
  logged at debug level and otherwise ignored. Each chunk must deliver
  exactly its range's byte count, checked at chunk completion.
- **Transient chunk failure**: per-chunk retry via the existing
  `maybeRetry` machinery (backoff, `filetransfer-retry-attempts`),
  unchanged; a partially delivered chunk resumes with a narrowed `Range`
  (offset by the bytes already received). A 206 response marks the item
  range-resumable even when the server omits `Accept-Ranges`. Retry
  exhaustion fails the whole transfer with the underlying
  `FileTransferError` — observably the same as a sequential download dying.
- **Object mutation**: chunk 0's `ETag` is recorded; later chunks send
  `If-Match` (skipped for weak etags, which can never match strong
  comparison). A `412` fails the chunk (4xx → no retry) and thus the
  download; additionally each chunk's response `ETag` is compared against
  chunk 0's and a mismatch aborts with a distinct "file changed on the
  server" error. Servers returning no ETag skip the check (NAR hash
  verification still guarantees end-to-end integrity).
- **Interrupts / sink failure**: `checkInterrupt()` in the drain loop, as
  today. On unwind the coordinator marks the transfer dead (callbacks
  discard further data) and unpauses a paused head chunk so it cannot pin a
  connection slot; in-flight chunks drain to completion in the background
  (there is no per-transfer cancellation in FileTransfer yet — pre-existing
  FIXME).

## Testing

- **Unit** (`src/libstore-tests/`): table-driven tests for the pure
  planning/window logic (chunk boundaries, last-chunk truncation, window
  top-up arithmetic, eligibility predicate), modeled on
  `filetransfer-retry.cc`. Expose internals via `filetransfer-impl.hh`
  precedent.
- **Integration**: small Range-capable HTTP stub server in
  `src/libstore-test-support` (the existing `openssl s_server -WWW` harness
  cannot serve ranges) with hooks for error injection, response delays and
  mid-body disconnects. Cover: 206 fan-out and reassembly; delayed
  out-of-order completion; 200 fallback with no extra requests; ETag
  mismatch abort; weak-etag If-Match skip; chunk retry after injected
  transient failure; mid-body disconnect with narrowed-Range resume;
  end-to-end substitution into a local store through a served binary cache
  with correct, missing, too-small and too-large narinfo `FileSize` (and a
  wrong `FileHash`), which supersedes the shell functional test originally
  planned here (no Range-capable server exists in `tests/functional/`, and
  the C++ test covers the same store-level path deterministically).
- **Docs**: setting description carries the memory formula; release note
  entry.

## Decisions log (from brainstorming)

- Dynamic upgrade of running downloads: achieved by chunking from the start
  and varying the in-flight window at chunk boundaries — no mid-stream
  connection aborts, no stream "conversion".
- One option only; threshold = chunk size; pool bound = `http-connections`.
- No per-file connection cap (YAGNI; `http-connections` bounds it; escape
  hatch exists if review demands one).
- No reassembly budget: window is the memory bound; formula documented.
- Plain default-off setting, no experimental-feature gate (may be added in
  upstream review as a one-liner).
