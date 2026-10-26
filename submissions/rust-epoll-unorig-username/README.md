# Rust + raw epoll + SQLite

| | |
|---|---|
| Language | Rust 1.90.0 (edition 2021), installed with rustup into `/opt/rust` |
| Framework | none: a hand-written HTTP/1.1 server on Linux `epoll` |
| Crates | `libc` 0.2.177 (pinned in `Cargo.lock`), nothing else |
| SQLite | 3.54.1, official amalgamation (SHA-256 checked), compiled by `build.sh` and linked statically |
| **Nginx or direct** | **Direct**: serves `HOST:PORT` (`0.0.0.0:80`) itself |

```bash
sudo bash install.sh   # build-essential, curl, unzip, rustup + Rust 1.90.0
bash build.sh          # SQLite static lib, then cargo build --release --locked
SQLITE_PATH=... JWT_SECRET=... HOST=0.0.0.0 PORT=80 bash start.sh
```

## Design

- **One thread, one level-triggered `epoll` loop.** The box has one vCPU, so extra threads would only
  add context switches. Each connection is a small record in a table indexed by fd; input and output
  buffers are allocated only when a request arrives split across reads or a response doesn't fit the
  socket buffer, so idle keep-alive connections cost almost nothing. Idle connections are never
  timed out.
- **Group commit per loop iteration.** All requests that arrive in one `epoll_wait` batch run inside one
  deferred SQLite transaction, opened on first use. Responses are written into one arena and sent only
  after `COMMIT` succeeds, so a 201 always means the row is durable (WAL, `synchronous=NORMAL`). If a
  commit fails, those connections are closed instead of being told something was saved. Sharing one
  transaction also saves the per-statement read-lock setup for the reads in the batch.
- **Checkpoints happen off the request path.** In WAL mode, SQLite copies the log back into the
  database every 1,000 pages and fsyncs it, inline in whichever `COMMIT` crosses the line. That stall
  hit every client in the batch and set the p99. Here a small helper process (the same binary, started
  with `--ckpt`) runs `PASSIVE` checkpoints in a loop. The event loop never waits on the disk, and the
  WAL stays a few MB under normal load. If the helper can't keep up (the CPU is pegged for minutes),
  the server's own `wal_autocheckpoint=65536` still caps the WAL at about 256 MB. Durability is
  unchanged: WAL with `synchronous=NORMAL`, and a response waits for its `COMMIT`.
- **SQLite settings**: WAL, `synchronous=NORMAL`, 1 GiB `mmap_size`, 16 MB page cache, normal locking.
  Compiled with `SQLITE_THREADSAFE=0` and other options that remove unused work (no mutexes, no memory
  statistics). Every query is a persistent prepared statement; every request reads the database while
  it is served.
- **Elastic workers past the descriptor limit.** Every keep-alive client holds a descriptor and
  `LimitNOFILE` applies per process. Normally there is exactly one server process (plus the
  checkpointer). When it gets within max(limit/16, 64) descriptors of its limit, it pauses its listener
  and `fork`+`exec`s one copy of itself in shared mode on the same port (`SO_REUSEPORT`). Once the copy
  says it is listening, the first process accepts what is queued and closes its listener, so new clients
  go to the copy; it reopens the listener when its own count falls by an eighth. Each copy does the same
  in turn. A write that finds its read snapshot stale (`SQLITE_BUSY_SNAPSHOT`) ends the read-only part
  of the batch, takes the write lock with `BEGIN IMMEDIATE` and retries. Workers and the checkpointer
  die with their parent (`PR_SET_PDEATHSIG`); only the first process starts a checkpointer.
- **Feed: newest ids first, then two range scans.** `/feed` first reads the 20 newest ids from the
  `(created_at, id)` index. When they span fewer than 256 ids (new posts get consecutive ids, so this is
  the normal case), one scan of `likes_post_id_idx` over that range counts the likes and one scan of
  `posts` over it fetches the rows, which are then put back in feed order. That replaces 20 separate
  count subqueries and 20 join lookups (about 12% less CPU per request on the mixed load). If the ids
  are spread out, or a row doesn't turn up, it runs the reference query instead. Both run in the
  batch's read transaction, so they see one snapshot.
- **SQL**: the reference post query (`count(*)` subquery on `likes_post_id_idx`). A like is one statement,
  `INSERT ... SELECT ... WHERE EXISTS (post) ON CONFLICT DO NOTHING`; a post lookup runs only if nothing
  was inserted, to tell "already liked" from "no such post".
- **JWT** verified on every request: HS256 only, constant-time signature compare, `exp`/`nbf` checked (`exp` is required).
  The HMAC key schedule (the SHA-256 state after the padded key) is computed once at startup; that is
  key setup, not a cache of verifications. SHA-256, base64url and a strict RFC 8259 JSON validator are
  written by hand (no serde), so request bodies and token parts are validated in one pass without
  allocating a document tree.
- **Responses**: compact JSON built directly into the output arena with the exact key order; the
  `Content-Length` is patched in after the body is written. Pipelined requests are answered in order
  with one `write` per connection per batch. `Expect: 100-continue` is honored.
- **Build**: `opt-level=3`, fat LTO, one codegen unit, `panic=abort`, `target-cpu=x86-64-v2`.

Body rules follow `String.prototype.trim` (the full JS whitespace set) and count Unicode code points
(what SQLite's `length()` CHECK counts). Lone UTF-16 surrogates in `\u` escapes become U+FFFD.
Chunked request bodies are not supported (out of scope; k6 and curl send `Content-Length`).

## License

MIT, under the repo's [license](../../LICENSE).
