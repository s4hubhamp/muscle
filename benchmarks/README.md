# Query benchmarks

Requires Zig 0.16.0 and macOS or Windows.
The comparison script uses Python 3 with no additional packages.

```sh
mkdir -p benchmarks/results
zig build bench -Doptimize=ReleaseFast -- --label baseline > benchmarks/results/baseline.json
# Make your changes, then run with the same compiler and options:
zig build bench -Doptimize=ReleaseFast -- --label candidate > benchmarks/results/candidate.json
python3 benchmarks/compare.py benchmarks/results/baseline.json benchmarks/results/candidate.json
```

Labels can record a commit SHA or describe uncommitted changes. Results are ignored
by Git. JSON goes to stdout; fixture progress and median timings go to stderr.
The build step runs again even when the benchmark executable is cached.
Compilation, database creation, and inserts are outside the measured intervals.
Fixture inserts execute in batches of 100 statements.

## Workloads

The fixture contains 10,000 sequential integer primary keys (1 through N), a
non-indexed score cycling from 0 to 99, and a fixed 36-byte text payload.
Every SELECT projects `id, score`:

| Name | Query shape | Queries per sample |
| --- | --- | --- |
| primary_key_hit | Equality lookup, cycling through 128 deterministic keys | 1024 |
| primary_key_miss | Equality lookup for N + 1 | 1024 |
| sequential_scan | Entire table | 10 |
| filtered_sequential_scan | Non-indexed `score = 42` | 10 |
| range_scan_narrow | Half-open primary-key interval of 10 rows | 100 |
| range_scan_wide | Half-open primary-key interval of N / 10 rows | 30 |

These are end-to-end SQL timings through `Muscle.execute`: parsing, analysis,
planning, storage access, and result materialization. They also include query
arena reset and checksum consumption of returned primary keys. They are not
isolated B-tree timings. Row counts are checked during each query; every SQL
statement's expected row count and first/last key are checked before timing.

Each run owns a unique `muscle-bench-*` temporary directory, removed after the
database closes, including on ordinary error returns. On Windows the parent is
`TEMP`, then `TMP`, with the working directory as a fallback. On macOS it is
`TMPDIR`, falling back to `/tmp`. No existing database is used.
Three untimed batches precede 15 timed batches per workload. Each sample records
nanoseconds per query, with a median, minimum, maximum, and observable checksum.
The same arena retains allocated capacity between queries to bound memory use.

## Options and comparisons

```sh
zig build bench -Doptimize=ReleaseFast -- --help
# Quick smoke run:
zig build bench -Doptimize=ReleaseFast -- --rows 100 --samples 3 --warmup 1
# A larger workload:
zig build bench -Doptimize=ReleaseFast -- --rows 50000 --samples 25 --warmup 5
```

Rows must be 100–1,000,000, samples 3–1,000, and warmup batches 1–1,000.
The comparison script rejects mismatched toolchains, build modes, target metadata,
configurations, suite versions, queries, iteration counts, or checksums. It
recomputes medians from raw samples and shows relative median absolute deviation
as a measure of sample spread. Negative percentage change means faster.

Use the same machine, power settings, Zig version, build flags, and workload
configuration. Alternate baseline/candidate runs when investigating small changes.
CPU metadata cannot establish that two runs used the same physical machine.
Warmups reuse the database and OS caches; datasets exceeding the buffer pool can
still require page reads. These benchmarks do not measure cold storage or write
commit performance. Changing fixture/query/measurement semantics should increment
`suite_version` in `src/bench.zig` to prevent misleading comparisons.
