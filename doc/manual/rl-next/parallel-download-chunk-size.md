---
synopsis: "New setting `parallel-download-chunk-size`: parallel downloads of single large files"
prs: []
---

Downloads of single large files from binary caches can now use multiple
HTTP connections. When the new `parallel-download-chunk-size` setting is set
to a nonzero size (e.g. `64M`), NARs exceeding that size are fetched as
parallel HTTP byte-range requests. The size comes from the `.narinfo`
(`FileSize`); caches that omit it are handled by using the first range
request as a size probe.

This helps on links where per-connection throughput is limited: substitution
already downloads many *paths* in parallel, but a single large NAR at the
end of a build was previously stuck on one connection.

The connection pool is still bounded by `http-connections`: a chunked
download only fans out over connection slots that are otherwise idle, and
falls back to as little as one connection when the pool is busy. Servers
without byte-range support transparently degrade to a normal
single-connection download.

The feature is off by default.
