<p align="center">
  <img src=".github/logo.svg" width="80" height="80" alt="lock logo">
</p>

<h1 align="center">@prsm/lock</h1>

Distributed locking primitives for Redis. Mutex for exclusive locks, semaphore for N concurrent leases. All operations are atomic via Lua scripts, and locks auto-expire on crash via TTL.

## Installation

```bash
npm install @prsm/lock
```

## Mutex

Exclusive lock. One holder at a time.

```js
import { mutex } from "@prsm/lock"

const lock = mutex({
  redis: { host: "127.0.0.1", port: 6379 },
})

const { acquired, id } = await lock.acquire("my-job", { ttl: "30s" })
if (!acquired) return

try {
  await doWork()
} finally {
  await lock.release("my-job", id)
}
```

### `mutex(options)`

- `redis` - `{ url?, host?, port?, password? }` passed to node-redis
- `prefix` - Redis key prefix (default `"lock:mutex:"`)

### Methods

| Method | Returns | Description |
|---|---|---|
| `acquire(key, opts?)` | `{ acquired, id }` | Attempt to acquire. `opts.ttl` (default `"10s"`), `opts.id` (custom holder ID) |
| `release(key, id)` | `boolean` | Release only if you still own it |
| `peek(key)` | `{ held, holder, ttl }` | Check lock state without acquiring |
| `close()` | `void` | Disconnect Redis |

## Semaphore

Up to N concurrent holders.

```js
import { semaphore } from "@prsm/lock"

const sem = semaphore({
  max: 20,
  ttl: "60s",
  redis: { host: "127.0.0.1", port: 6379 },
})

const { acquired, id } = await sem.acquire("worker-slots")
if (!acquired) return

const heartbeat = setInterval(() => sem.renew("worker-slots", id), 15000)

try {
  await processTask()
} finally {
  clearInterval(heartbeat)
  await sem.release("worker-slots", id)
}
```

### `semaphore(options)`

- `max` - Maximum concurrent holders (required)
- `ttl` - Lease lifetime (default `"60s"`). Expired leases are pruned automatically
- `redis` - Same as mutex
- `prefix` - Redis key prefix (default `"lock:sem:"`)

### Methods

| Method | Returns | Description |
|---|---|---|
| `acquire(key, opts?)` | `{ acquired, id }` | Acquire a lease slot. `opts.id` for custom lease ID |
| `release(key, id)` | `true` | Release a lease |
| `renew(key, id)` | `boolean` | Extend lease lifetime. `false` if expired |
| `count(key)` | `number` | Active lease count (after pruning) |
| `peek(key)` | `{ active, max, available, holders }` | Full semaphore state |
| `close()` | `void` | Disconnect Redis |

## How It Works

**Mutex** uses `SET key value NX PX ttl` for atomic acquire and a Lua script for ownership-checked release (only the holder can release).

**Semaphore** uses a Redis sorted set where each member is a lease ID and the score is a timestamp. Expired entries are pruned via `ZREMRANGEBYSCORE` before each acquire. Renewal updates the timestamp to extend the lease window.

Both are crash-tolerant via TTLs. If a holder dies without releasing, the lock/lease expires automatically.
