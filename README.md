# Distributed Key-Value Store

A persistent key-value store built in Go, featuring write-ahead logging,
snapshotting, and crash recovery. Raft-based replication in progress.

## Features
- gRPC API (Set / Get / Delete)
- Write-Ahead Log with batched fsync for durability
- Periodic snapshotting with log compaction
- Crash recovery (snapshot + WAL replay on startup)
- [WIP] Raft consensus for replication

## Architecture

```
Client --gRPC (Set/Get/Delete)--> KVStoreService --> KVStoreData (map + lock)
                                                          |          |
                                                         WAL    every 1000 ops
                                                     (wal.log)       |
                                                          |          v
                                                          |    snapshot.json
                                                          |    + WAL truncated
                                                          |    up to that point
                                                          v
                                            replayed on startup, after
                                            the snapshot is loaded
```

## Benchmarks (single-node, Intel i5-1145G7 @ 2.60GHz, 4C/8T, 16GB RAM)

| Mode  | Concurrency | P50     | P99      | Throughput   |
|-------|-------------|---------|----------|--------------|
| Write | 1           | 553µs   | 12.6ms   | 1,337 ops/s  |
| Write | 10          | 800µs   | 6.2ms    | 7,637 ops/s  |
| Write | 50          | 4.76ms  | 75.3ms   | 10,290 ops/s |
| Write | 100         | 10.6ms  | 101.4ms  | 9,991 ops/s  |
| Write | 200         | 25.3ms  | 174.7ms  | 10,495 ops/s |

Throughput tops out around 10k ops/s once you hit ~50 concurrent
clients, more concurrency doesn't help after that, it just queues
requests and drives latency up (P50 goes from under 1ms at low
concurrency to 25ms at 200 clients for basically the same ops/s). Read
benchmarking isn't wired up in `bench/` yet, only writes are measured
for now.

P99 used to be around 8 seconds under load before batching the WAL
fsync every 100 writes instead of on every single write (same tradeoff
as Redis' `appendfsync everysec`). The occasional double-digit ms P99
at low concurrency lines up with a snapshot writing to disk.

Reproduce it yourself:
```
go run ./bench -mode=latency -n=100000 -concurrency=1
go run ./bench -mode=throughput -levels=1,10,50,100,200 -duration=5s
```

## Running

```
cd deployment
docker compose up
```

Server listens on `:50051`. `wal.log` and `snapshot.json` live in a
named Docker volume (`kvstore-data`) so data survives a container
restart.

Without Docker:
```
go run ./cmd/kvstore
```

## Design notes

- **Durability tradeoff**: fsync is batched every 100 writes instead of
  every single one. Means up to 99 acked writes could be lost if the OS
  crashes between syncs, but it's what got P99 down from seconds to
  milliseconds under load.
- **Crash-consistent snapshots**: Set/Remove hold one lock across both
  the WAL append and the map update, so a snapshot can never catch a
  write that's in the WAL but not in the map yet (or the other way
  around). The WAL keeps track of its own byte offset. A snapshot copies
  the map and grabs that offset under a short lock, writes the JSON file
  with no lock held, then only truncates the WAL bytes it already
  captured, so writes that land while the file is being written don't
  get lost.
- **Recovery**: on startup the snapshot loads first, then the WAL gets
  replayed on top of it. Since the WAL is truncated up to each
  snapshot's offset, it only ever holds writes made after the last
  snapshot, so this rebuilds the exact state from before the crash.
- **WAL format**: binary, length-prefixed (op byte + key/value lengths +
  raw bytes) instead of delimited text, so keys or values with spaces or
  newlines in them round-trip correctly.
