# MuscleDB

A database written in Zig.

Requires Zig 0.16.0. Storage I/O supports macOS and Windows, using exclusive
nonblocking file locks and positional page reads/writes. Database paths passed to
`Muscle.init` must be absolute; journal files live alongside the database.

Run correctness checks with `zig build test`.

Start the HTTP query server with:

```sh
zig build run -- --db muscle.db --host 127.0.0.1 --port 8080
```

These are the defaults; relative database paths resolve from the working directory.
Send raw SQL in the body of `POST /query`:

```sh
curl --data-binary 'create table users (id int primary key, name text(64))' http://127.0.0.1:8080/query
curl --data-binary "insert into users (id, name) values (1, 'Alice')" http://127.0.0.1:8080/query
curl --data-binary 'select * from users' http://127.0.0.1:8080/query
```

Responses use the database's existing plain-text table/error format. Successful
queries return HTTP 200, SQL errors return 400, and internal failures return 500.
Queries are limited to 1 MiB. The server processes one connection at a time and
closes each connection after its response. It has no authentication; use a trusted
local environment or protect access before binding to a public interface.

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
