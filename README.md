# MuscleDB

A database written in Zig.

Requires Zig 0.16.0. Storage I/O supports macOS and Windows, using exclusive
nonblocking file locks and positional page reads/writes. Database paths passed to
`Muscle.init` must be absolute; journal files live alongside the database.

Run correctness checks with `zig build test`.

Run query benchmarks with `zig build bench -Doptimize=ReleaseFast`.
See [the benchmark guide](benchmarks/README.md) for workloads, saved baselines,
and performance comparisons.

On Windows, run the same `zig build test` and `zig build bench` commands locally.
To compile Windows tests and benchmarks from another platform without running them:

```sh
zig build test-compile bench-build -Dtarget=x86_64-windows -Doptimize=ReleaseSafe
```

Use `-Dtarget=aarch64-windows` for Windows on ARM. Cross-compilation checks code
generation; file locking, reopening, and rollback tests must also run on Windows
to validate native filesystem behavior.
