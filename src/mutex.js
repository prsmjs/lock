import { createClient } from "redis"
import ms from "@prsm/ms"
import crypto from "node:crypto"

const RELEASE_SCRIPT = `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`

/**
 * @typedef {Object} RedisOptions
 * Connection settings forwarded as-is to node-redis `createClient`. Omit to connect to redis on localhost:6379.
 * @property {string} [url] - full connection string, e.g. `redis://user:pass@host:6379/0`. Takes precedence over the discrete host/port fields below
 * @property {string} [host] - redis host (default `127.0.0.1`)
 * @property {number} [port] - redis port (default `6379`)
 * @property {string} [password] - password, if the server requires auth
 */

/**
 * @typedef {Object} MutexOptions
 * @property {RedisOptions} [redis] - redis connection settings; the mutex opens and manages its own connection
 * @property {string} [prefix] - key prefix for every lock this manager stores (default `"lock:mutex:"`, so a lock named `job` lives at `lock:mutex:job`)
 * @property {object} [tracer] - optional `@prsm/trace` tracer; when set, `acquire` and `release` run inside spans
 */

/**
 * @typedef {Object} AcquireOptions
 * @property {string|number} [ttl] - how long the lock survives if it is never released, as a duration string (`"30s"`, `"5m"`) or milliseconds (default `"10s"`). This is a crash safety net: if the holder dies, the lock auto-expires instead of deadlocking, so set it longer than you expect to hold the lock
 * @property {string} [id] - holder id to claim the lock under (default: a random UUID). Pass a deterministic id for idempotent locking, e.g. a per-tick id so the same logical attempt reuses one id
 */

/**
 * @typedef {Object} AcquireResult
 * @property {boolean} acquired - whether the lock was taken; `false` means someone else holds it (the call never blocks or retries)
 * @property {string|null} id - the holder id to hand back to `release`, or `null` when not acquired
 */

/**
 * @typedef {Object} PeekResult
 * @property {boolean} held - whether the lock is currently held by anyone
 * @property {string|null} holder - the current holder's id, or `null` if unheld
 * @property {number} ttl - milliseconds until the lock expires, or `-1` if unheld
 */

/**
 * @typedef {Object} MutexLockInfo
 * @property {string} key - the lock name with the manager's prefix stripped off
 * @property {string} holder - the holder id currently owning this lock
 * @property {number} ttl - milliseconds until this lock expires
 */

/**
 * Create an exclusive lock manager: at most one holder per key at a time. Acquire
 * is non-blocking - it either takes the lock or returns immediately.
 * @param {MutexOptions} [options]
 */
export function mutex(options = {}) {
  const prefix = options.prefix ?? "lock:mutex:"
  const tracer = options.tracer ?? null
  const client = createClient(options.redis ?? {})
  let connectPromise = null

  async function ensureConnected() {
    if (!connectPromise) {
      connectPromise = client.connect()
    }
    await connectPromise
  }

  async function _acquire(key, opts = {}) {
    await ensureConnected()
    const ttl = ms(opts.ttl ?? "10s")
    const id = opts.id ?? crypto.randomUUID()
    const result = await client.set(`${prefix}${key}`, id, { NX: true, PX: ttl })
    return { acquired: result === "OK", id: result === "OK" ? id : null }
  }

  async function _release(key, id) {
    await ensureConnected()
    const result = await client.eval(RELEASE_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [id],
    })
    return result === 1
  }

  /**
   * Try to take the lock on `key`. Non-blocking: returns immediately whether or not it succeeded.
   * @param {string} key - lock name (stored at `${prefix}${key}`)
   * @param {AcquireOptions} [opts]
   * @returns {Promise<AcquireResult>}
   */
  async function acquire(key, opts = {}) {
    if (!tracer) return _acquire(key, opts)
    return tracer.span('lock.mutex.acquire', { 'lock.key': key }, async (span) => {
      const r = await _acquire(key, opts)
      span.setAttribute('lock.acquired', r.acquired)
      return r
    })
  }

  /**
   * Release a lock, but only if `id` still owns it (ownership-checked). Safe to call
   * after the ttl expired - it returns `false` without disturbing a newer holder.
   * @param {string} key - lock name
   * @param {string} id - the holder id returned by `acquire`
   * @returns {Promise<boolean>} true if this caller held the lock and released it
   */
  async function release(key, id) {
    if (!tracer) return _release(key, id)
    return tracer.span('lock.mutex.release', { 'lock.key': key }, () => _release(key, id))
  }

  /**
   * Inspect a lock without acquiring it.
   * @param {string} key - lock name
   * @returns {Promise<PeekResult>}
   */
  async function peek(key) {
    await ensureConnected()
    const fullKey = `${prefix}${key}`
    const [holder, ttl] = await Promise.all([
      client.get(fullKey),
      client.pTTL(fullKey),
    ])
    if (holder === null) {
      return { held: false, holder: null, ttl: -1 }
    }
    return { held: true, holder, ttl }
  }

  /**
   * List every lock currently held under this manager's prefix.
   * @returns {Promise<MutexLockInfo[]>}
   */
  async function list() {
    await ensureConnected()
    const results = []
    let cursor = "0"
    do {
      const reply = await client.scan(cursor, { MATCH: `${prefix}*`, COUNT: 200 })
      cursor = String(reply.cursor)
      for (const fullKey of reply.keys) {
        const key = fullKey.slice(prefix.length)
        const [holder, ttl] = await Promise.all([
          client.get(fullKey),
          client.pTTL(fullKey),
        ])
        if (holder !== null) results.push({ key, holder, ttl })
      }
    } while (cursor !== "0")
    return results
  }

  /**
   * Disconnect the redis client. The manager is unusable afterward.
   * @returns {Promise<void>}
   */
  async function close() {
    if (connectPromise) {
      await connectPromise
      await client.quit()
      connectPromise = null
    }
  }

  client.on("error", () => {})

  return { acquire, release, peek, list, close }
}
