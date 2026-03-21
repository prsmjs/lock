# @prsm/lock

distributed locking primitives for Redis. mutex (exclusive lock) and semaphore (N concurrent leases). atomic, safe, crash-tolerant.

## why this exists

three packages in the @prsm ecosystem independently implement the same Redis locking patterns:

- **@prsm/cron** (`/Users/jonathanpyers/code/cron`): uses `SET NX PX` + Lua ownership-checked release for tick locks and exclusive job locks
- **@prsm/cell** (`/Users/jonathanpyers/code/cells`): uses the exact same `SET NX PX` + Lua release pattern for compute locks and poll locks
- **@prsm/queue** (`/Users/jonathanpyers/code/queue`): uses a ZSET-based semaphore with heartbeat renewal for global concurrency control

the mutex code in cron and cell is virtually identical - same Lua script, same pattern, copy-pasted. the semaphore in queue is a different pattern but solves a related problem. both belong in a shared primitive.

this package extracts both patterns into a single, well-tested library that all three packages (and future packages) can depend on.

## what to build

two primitives:

### 1. mutex - exclusive lock

exactly one holder at a time. used when you need "only one instance should do this."

current pattern (duplicated in cron and cell):
```js
// acquire
const acquired = await redis.set(lockKey, instanceId, { NX: true, PX: ttlMs })

// release (Lua - only delete if we still own it)
await redis.eval(
  `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`,
  { keys: [lockKey], arguments: [instanceId] }
)
```

### 2. semaphore - N concurrent leases

up to N holders at a time. used when you need "at most N instances should do this concurrently."

current pattern (in queue):
```js
// acquire (Lua - prune expired, check capacity, add lease)
const ACQUIRE_SCRIPT = `
local key = KEYS[1]
local max = tonumber(ARGV[1])
local id = ARGV[2]
local ttl = tonumber(ARGV[3])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - ttl)
if redis.call('ZCARD', key) < max then
  redis.call('ZADD', key, now, id)
  return 1
end
return 0
`

// release (Lua - remove lease)
const RELEASE_SCRIPT = `
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

// renew (Lua - update timestamp if still holding)
const RENEW_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], now, ARGV[1])
  return 1
end
return 0
`
```

## design principles

- two primitives, nothing else. mutex and semaphore. no distributed read/write locks, no reentrant locks, no fairness queues. those can be added later if needed
- each factory manages its own Redis connection (consistent with @prsm/limit, @prsm/cron, etc)
- atomic operations via Lua scripts. no race conditions
- crash-tolerant via TTLs. if a holder dies without releasing, the lock expires
- ownership-verified release. you can only release a lock you hold. prevents accidental release after TTL expiry when another holder has acquired

## tech constraints

- plain javascript, ESM, no typescript, no build step
- package ships raw .js files
- runtime dependencies: `redis` (node-redis), `@prsm/ms` (duration parsing)
- node >= 20
- vitest for tests

## package setup

- name: `@prsm/lock`
- `"type": "module"` in package.json
- single entry point: `import { mutex, semaphore } from "@prsm/lock"`

## API

### `mutex(options)` - create an exclusive lock manager

```js
import { mutex } from "@prsm/lock"

const lock = mutex({
  redis: { host: "127.0.0.1", port: 6379 },
  prefix: "myapp:",     // optional, default "lock:mutex:"
})
```

options:
- `redis` - `{ url?, host?, port?, password? }` passed to node-redis `createClient`
- `prefix` - optional string prefix for Redis keys. default `"lock:mutex:"`

returns an object with these methods:

#### `lock.acquire(key, options?)`

```js
const result = await lock.acquire("job:cleanup", { ttl: "30s" })
// { acquired: true, id: "a1b2c3..." }
// or
// { acquired: false, id: null }
```

attempts to acquire an exclusive lock on `key`. returns immediately (non-blocking). does not wait/retry.

- `key` - string. the lock name. stored at `${prefix}${key}` in Redis
- `options.ttl` - duration string or ms. how long the lock lives if not released. default `"10s"`. this is a safety net for crashes - always set it longer than you expect to hold the lock
- `options.id` - optional string. custom lock holder ID. default: `crypto.randomUUID()`. useful when you want a deterministic ID (e.g. tick-based IDs for idempotent locking like cron does)

returns `{ acquired: boolean, id: string | null }`. `id` is the holder ID if acquired (needed for release), `null` if not acquired.

#### `lock.release(key, id)`

```js
const released = await lock.release("job:cleanup", result.id)
// true if we held the lock and released it
// false if the lock was already expired or held by someone else
```

releases the lock, but only if we still own it (ownership check via Lua). safe to call even if the lock expired - it will return `false` without affecting the new holder.

#### `lock.peek(key)`

```js
const info = await lock.peek("job:cleanup")
// { held: true, holder: "a1b2c3...", ttl: 24531 }
// or
// { held: false, holder: null, ttl: -1 }
```

check if a lock is currently held without acquiring it. returns holder ID and remaining TTL in ms.

#### `lock.close()`

```js
await lock.close()
```

disconnect the Redis client. the lock manager is unusable after this.

---

### `semaphore(options)` - create a concurrent lease manager

```js
import { semaphore } from "@prsm/lock"

const sem = semaphore({
  max: 20,
  ttl: "60s",
  redis: { host: "127.0.0.1", port: 6379 },
  prefix: "myapp:",     // optional, default "lock:sem:"
})
```

options:
- `max` - number. maximum concurrent lease holders
- `ttl` - duration string or ms. lease lifetime. if a holder doesn't renew or release within this window, the lease is considered expired and pruned on next acquire. default `"60s"`
- `redis` - same as mutex
- `prefix` - optional, default `"lock:sem:"`

returns an object with these methods:

#### `sem.acquire(key, options?)`

```js
const result = await sem.acquire("api:external")
// { acquired: true, id: "x9y8z7..." }
// or
// { acquired: false, id: null }
```

attempts to acquire one lease slot. prunes expired leases first (via ZREMRANGEBYSCORE), then checks if count < max.

- `key` - string. the semaphore name. stored at `${prefix}${key}` in Redis (as a sorted set)
- `options.id` - optional custom lease ID. default: `crypto.randomUUID()`

returns `{ acquired: boolean, id: string | null }`.

#### `sem.release(key, id)`

```js
const released = await sem.release("api:external", result.id)
// true
```

removes the lease from the sorted set. always returns true (ZREM is idempotent - removing a non-existent member is fine).

#### `sem.renew(key, id)`

```js
const renewed = await sem.renew("api:external", result.id)
// true if the lease existed and was renewed
// false if the lease was already expired/removed
```

updates the lease timestamp to `now`, effectively extending its lifetime by another `ttl` window. use this on a heartbeat interval (e.g. every `ttl / 3` ms) to keep long-running leases alive.

#### `sem.count(key)`

```js
const active = await sem.count("api:external")
// 12 (number of currently active leases after pruning expired ones)
```

returns the number of active (non-expired) leases. prunes expired entries first.

#### `sem.peek(key)`

```js
const info = await sem.peek("api:external")
// { active: 12, max: 20, available: 8, holders: ["id1", "id2", ...] }
```

returns current semaphore state after pruning expired entries.

#### `sem.close()`

```js
await sem.close()
```

disconnect the Redis client.

## usage patterns

### pattern 1: simple exclusive lock (what cron and cell do)

```js
import { mutex } from "@prsm/lock"

const lock = mutex({ redis: { host: "127.0.0.1", port: 6379 } })

const { acquired, id } = await lock.acquire("compute:total:gen42", { ttl: "30s" })
if (!acquired) return

try {
  await doExpensiveWork()
} finally {
  await lock.release("compute:total:gen42", id)
}
```

### pattern 2: tick-based idempotent locking (what cron does for scheduled jobs)

```js
const tickId = Math.floor(Date.now() / intervalMs)
const { acquired, id } = await lock.acquire(`job:cleanup:${tickId}`, {
  ttl: Math.max(intervalMs, 1000),
  id: instanceId,
})

if (acquired) {
  await runJob()
  // no need to release - the tick lock key is unique per tick
  // and will expire via TTL. but releasing is fine too
}
```

### pattern 3: exclusive long-running job (what cron does with exclusive: true)

```js
const { acquired, id } = await lock.acquire("reports:generate", { ttl: "10m" })
if (!acquired) return

try {
  await generateReports() // may take several minutes
} finally {
  await lock.release("reports:generate", id)
}
```

### pattern 4: global concurrency control (what queue does)

```js
import { semaphore } from "@prsm/lock"

const sem = semaphore({
  max: 20,
  ttl: "60s",
  redis: { host: "127.0.0.1", port: 6379 },
})

const { acquired, id } = await sem.acquire("worker-slots")
if (!acquired) {
  // all 20 slots are taken across all instances
  return
}

// start heartbeat to keep lease alive during long work
const heartbeat = setInterval(async () => {
  const ok = await sem.renew("worker-slots", id)
  if (!ok) clearInterval(heartbeat) // lease lost
}, 15_000)

try {
  await processTask()
} finally {
  clearInterval(heartbeat)
  await sem.release("worker-slots", id)
}
```

## testing

use vitest. redis must be running on localhost:6379. each test file should flush its redis DB in beforeEach.

### mutex tests

1. **acquire/release** - basic acquire returns `{ acquired: true }`, release returns true
2. **exclusivity** - second acquire on same key returns `{ acquired: false }`
3. **release enables re-acquire** - after release, another acquire succeeds
4. **TTL expiry** - lock auto-expires after TTL, then re-acquirable (use short TTL + delay)
5. **ownership-checked release** - releasing with wrong ID returns false, lock stays held
6. **release after expiry is safe** - releasing an expired lock doesn't affect new holder
7. **custom ID** - acquire with custom ID, release with that ID works
8. **peek** - returns correct held/holder/ttl state
9. **peek on unheld lock** - returns `{ held: false }`
10. **prefix isolation** - two mutex instances with different prefixes don't interfere

### semaphore tests

1. **acquire up to max** - N acquires succeed, N+1 fails
2. **release frees a slot** - after release, a new acquire succeeds
3. **TTL expiry frees slots** - expired leases are pruned, slots become available
4. **renew extends lease** - renewed lease doesn't expire at original time
5. **renew on expired lease** - returns false
6. **count** - returns correct active count after acquires, releases, and expiries
7. **peek** - returns correct active/max/available/holders
8. **concurrent acquire from multiple "instances"** - simulate with multiple semaphore objects sharing Redis
9. **heartbeat pattern** - lease stays alive across multiple TTL windows with renewal

## structure

```
src/
  index.js      - exports { mutex, semaphore }
  mutex.js      - mutex factory
  semaphore.js  - semaphore factory
tests/
  mutex.test.js
  semaphore.test.js
compose.yml     - Redis for local dev/tests
Makefile
package.json
CLAUDE.md
```

## important implementation notes

- all Redis operations that involve read-then-write must be Lua scripts (atomic). never do separate GET then SET - that's a race condition
- mutex uses simple string keys with SET NX PX. semaphore uses sorted sets (ZSET) where score = timestamp and member = lease ID
- for semaphore, always prune expired entries (ZREMRANGEBYSCORE) before checking count. this is done inside the acquire Lua script, not as a separate call
- use `redis.call('TIME')` inside Lua scripts for the timestamp, not a timestamp passed from Node. this avoids clock skew between the app server and Redis
- the `close()` method should QUIT the Redis client gracefully
- mutex `peek` uses GET + PTTL (two Redis calls, but peek is informational only so atomicity isn't critical)
- duration strings are parsed by @prsm/ms (e.g. "30s", "5m", "1h")
- generate holder IDs with `crypto.randomUUID()` by default

## after building @prsm/lock: integrate into existing packages

once @prsm/lock is built and tested, the next step is to integrate it as a dependency into the three packages that currently duplicate these patterns. this section explains exactly what to replace in each package.

### integrate into @prsm/cron (`/Users/jonathanpyers/code/cron`)

**file to modify:** `src/cron.js`

cron uses two locking patterns:

1. **tick lock** (line ~179-184): prevents duplicate execution of the same job tick across instances
   ```js
   // current code
   const lockKey = `${this._prefix}lock:${name}:${tickId}`
   const lockTtl = job.type === "interval" ? Math.max(job.interval, 1000) : 60000
   const acquired = await this._redis.set(lockKey, this._instanceId, { NX: true, PX: lockTtl })
   ```
   replace with `mutex.acquire()`

2. **exclusive lock** (line ~192-215): prevents overlapping execution when `exclusive: true`
   ```js
   // current code - acquire
   const key = `${this._prefix}running:${name}`
   const exclusiveAcquired = await this._redis.set(key, this._instanceId, { NX: true, PX: job.exclusiveTtl })

   // current code - release (Lua)
   await this._redis.eval(
     `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`,
     { keys: [key], arguments: [this._instanceId] }
   )
   ```
   replace with `mutex.acquire()` and `mutex.release()`

**approach:**
- add `@prsm/lock` as a dependency
- create a mutex instance inside the Cron constructor, reusing the same Redis connection options that cron already accepts. important: the mutex should use cron's existing prefix so Redis keys don't change (e.g. prefix `"cron:"` so tick locks remain at `cron:lock:jobname:tickid`)
- replace the raw SET NX PX calls with `mutex.acquire()`
- replace the raw Lua eval calls with `mutex.release()`
- the mutex instance should be closed in `cron.stop()`

**important:** the Redis key patterns must stay the same so existing deployments don't break. the tick lock key should still be `${prefix}lock:${name}:${tickId}` and the exclusive lock key should still be `${prefix}running:${name}`. this means you'll use the mutex with a prefix that matches cron's existing prefix, and pass the rest of the key path as the `key` argument to `acquire`/`release`.

**testing:** run `make test` in `/Users/jonathanpyers/code/cron`. all existing tests must pass. do not modify the tests.

### integrate into @prsm/cell (`/Users/jonathanpyers/code/cells`)

**file to modify:** `src/redis.js`

cell uses two locking patterns, both identical to cron's mutex:

1. **compute lock** (line ~92): prevents duplicate computation of the same cell
   ```js
   // current code
   const result = await client.set(`${prefix}${key}`, instanceId, { NX: true, PX: ttlMs })
   ```

2. **release** (line ~98-101): ownership-checked release via Lua
   ```js
   // current code
   const RELEASE_SCRIPT = `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`
   await client.eval(RELEASE_SCRIPT, {
     keys: [`${prefix}${key}`],
     arguments: [instanceId],
   }).catch(() => {})
   ```

**approach:**
- add `@prsm/lock` as a dependency
- cell's `src/redis.js` exports `acquireLock` and `releaseLock` functions. replace their internals with calls to a mutex instance
- the mutex needs to share cell's existing Redis client (cell already creates and manages its own Redis connection in `createRedisManager`). this is the tricky part - the mutex factory creates its own connection by default. two options:
  - option A: let the mutex create its own connection (simple but means an extra Redis connection per cell graph). this is probably fine
  - option B: refactor mutex to optionally accept an existing Redis client. this is cleaner but changes the mutex API
  - **go with option A** - an extra connection is not a problem and keeps the API simple
- cell's lock key patterns should stay the same: `${prefix}lock:compute:${name}:${generation}` for compute locks and `${prefix}poll:${name}:${tickId}` for poll locks

**testing:** run `make test` in `/Users/jonathanpyers/code/cells`. all existing tests must pass. do not modify the tests. if any tests currently test locking behavior directly (mocking Redis set/eval), those tests may need adjustment to work with the new dependency, but the *behavior* must remain identical.

### integrate into @prsm/queue (`/Users/jonathanpyers/code/queue`)

**file to modify:** `src/queue.js`

queue uses a semaphore pattern for global concurrency (lines ~34-65):

```js
// current code - three Lua scripts
const ACQUIRE_SCRIPT = `...` // ZSET-based acquire
const RELEASE_SCRIPT = `...` // ZREM
const RENEW_SCRIPT = `...`   // timestamp refresh
```

these are used in:
- `_acquireGlobalSlot()` (line ~464)
- `_releaseGlobalSlot()` (line ~490)
- `_renewGlobalSlot()` (line ~498)

**approach:**
- add `@prsm/lock` as a dependency
- create a semaphore instance inside Queue when `globalConcurrency > 0`
- replace `_acquireGlobalSlot`, `_releaseGlobalSlot`, and `_renewGlobalSlot` with calls to `sem.acquire()`, `sem.release()`, and `sem.renew()`
- remove the three Lua script constants (ACQUIRE_SCRIPT, RELEASE_SCRIPT, RENEW_SCRIPT)
- the semaphore instance should be closed in `queue.close()`
- **go with option A** again - let the semaphore create its own connection

**important:** the Redis key must stay the same: `"queue:active"`. configure the semaphore prefix accordingly.

**testing:** run `make test` in `/Users/jonathanpyers/code/queue`. all existing tests must pass. do not modify the tests.

## general integration notes

- do NOT change any test files in any of the three packages. the tests are the specification - if they pass, the integration is correct
- do NOT change any public API of any of the three packages. this is an internal refactor only
- the goal is to remove duplicated locking code and replace it with calls to @prsm/lock, with zero behavior changes
- after integration, each package's `package.json` should list `@prsm/lock` as a dependency. use a file path dependency for now: `"@prsm/lock": "file:../lock"` (all packages are siblings under `~/code/`)
- run each package's tests independently after integration to verify
