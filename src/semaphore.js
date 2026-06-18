import { createClient } from "redis"
import ms from "@prsm/ms"
import crypto from "node:crypto"

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

const RELEASE_SCRIPT = `
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

const RENEW_SCRIPT = `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local ttl = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - ttl)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], now, ARGV[1])
  return 1
end
return 0
`

const COUNT_SCRIPT = `
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - ttl)
return redis.call('ZCARD', key)
`

const PEEK_SCRIPT = `
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - ttl)
local members = redis.call('ZRANGE', key, 0, -1)
local active = #members
return {active, max, max - active, unpack(members)}
`

/**
 * @typedef {import('./mutex.js').RedisOptions} RedisOptions
 * @typedef {import('./mutex.js').AcquireResult} AcquireResult
 */

/**
 * @typedef {Object} SemaphoreOptions
 * @property {number} max - maximum number of concurrent lease holders across every instance sharing this redis key
 * @property {string|number} [ttl] - lease lifetime as a duration string (`"60s"`) or milliseconds (default `"60s"`). A lease that isn't renewed or released within this window is treated as expired and pruned on the next operation, so set it longer than a unit of work and renew long-running leases on a heartbeat (e.g. every ttl/3)
 * @property {RedisOptions} [redis] - redis connection settings; the semaphore opens and manages its own connection
 * @property {string} [prefix] - key prefix for the redis sorted sets backing each semaphore (default `"lock:sem:"`)
 * @property {object} [tracer] - optional `@prsm/trace` tracer; when set, `acquire`, `release`, and `renew` run inside spans
 */

/**
 * @typedef {Object} SemaphoreAcquireOptions
 * @property {string} [id] - lease id to claim (default: a random UUID). Keep it to renew or release this specific lease later
 */

/**
 * @typedef {Object} SemaphorePeekResult
 * @property {number} active - number of live (non-expired) leases right now
 * @property {number} max - the configured ceiling
 * @property {number} available - free slots remaining (`max - active`)
 * @property {string[]} holders - ids of the current lease holders
 */

/**
 * @typedef {Object} SemaphoreLockInfo
 * @property {string} key - the semaphore name with the prefix stripped off
 * @property {number} active - live lease count
 * @property {number} max - the configured ceiling
 * @property {number} available - free slots (`max - active`)
 * @property {string[]} holders - ids of the current lease holders
 */

/**
 * Create a concurrent lease manager: up to `max` holders per key at a time. Acquire
 * is non-blocking - it takes a slot if one is free or returns immediately.
 * @param {SemaphoreOptions} options
 */
export function semaphore(options = {}) {
  if (!options.max || options.max < 1) throw new Error("max must be a positive number")

  const max = options.max
  const ttlMs = ms(options.ttl ?? "60s")
  const prefix = options.prefix ?? "lock:sem:"
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
    const id = opts.id ?? crypto.randomUUID()
    const result = await client.eval(ACQUIRE_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(max), id, String(ttlMs)],
    })
    return { acquired: result === 1, id: result === 1 ? id : null }
  }

  async function _release(key, id) {
    await ensureConnected()
    await client.eval(RELEASE_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [id],
    })
    return true
  }

  async function _renew(key, id) {
    await ensureConnected()
    const result = await client.eval(RENEW_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [id, String(ttlMs)],
    })
    return result === 1
  }

  /**
   * Try to take one lease slot on `key`. Prunes expired leases first, then grants a
   * slot if fewer than `max` are active. Non-blocking.
   * @param {string} key - semaphore name (stored at `${prefix}${key}`)
   * @param {SemaphoreAcquireOptions} [opts]
   * @returns {Promise<AcquireResult>}
   */
  async function acquire(key, opts = {}) {
    if (!tracer) return _acquire(key, opts)
    return tracer.span('lock.semaphore.acquire', { 'lock.key': key, 'lock.max': max }, async (span) => {
      const r = await _acquire(key, opts)
      span.setAttribute('lock.acquired', r.acquired)
      return r
    })
  }

  /**
   * Release a lease, freeing its slot. Idempotent - releasing an already-gone lease is a no-op.
   * @param {string} key - semaphore name
   * @param {string} id - the lease id returned by `acquire`
   * @returns {Promise<boolean>}
   */
  async function release(key, id) {
    if (!tracer) return _release(key, id)
    return tracer.span('lock.semaphore.release', { 'lock.key': key }, () => _release(key, id))
  }

  /**
   * Extend a lease by another ttl window. Call on a heartbeat to keep a long-running
   * lease alive; returns `false` if the lease already expired or was released.
   * @param {string} key - semaphore name
   * @param {string} id - the lease id to renew
   * @returns {Promise<boolean>}
   */
  async function renew(key, id) {
    if (!tracer) return _renew(key, id)
    return tracer.span('lock.semaphore.renew', { 'lock.key': key }, () => _renew(key, id))
  }

  /**
   * Count live leases on `key` (expired leases are pruned first).
   * @param {string} key - semaphore name
   * @returns {Promise<number>}
   */
  async function count(key) {
    await ensureConnected()
    const result = await client.eval(COUNT_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(ttlMs)],
    })
    return result
  }

  /**
   * Inspect a semaphore's current state without taking a slot.
   * @param {string} key - semaphore name
   * @returns {Promise<SemaphorePeekResult>}
   */
  async function peek(key) {
    await ensureConnected()
    const result = await client.eval(PEEK_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(ttlMs), String(max)],
    })
    const active = result[0]
    const maxVal = result[1]
    const available = result[2]
    const holders = result.slice(3)
    return { active, max: maxVal, available, holders }
  }

  /**
   * List every semaphore with at least one live lease under this manager's prefix.
   * @returns {Promise<SemaphoreLockInfo[]>}
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
        const result = await client.eval(PEEK_SCRIPT, {
          keys: [fullKey],
          arguments: [String(ttlMs), String(max)],
        })
        const active = result[0]
        if (active > 0) {
          results.push({
            key,
            active,
            max: result[1],
            available: result[2],
            holders: result.slice(3),
          })
        }
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

  return { acquire, release, renew, count, peek, list, close }
}
