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

  async function acquire(key, opts = {}) {
    if (!tracer) return _acquire(key, opts)
    return tracer.span('lock.semaphore.acquire', { 'lock.key': key, 'lock.max': max }, async (span) => {
      const r = await _acquire(key, opts)
      span.setAttribute('lock.acquired', r.acquired)
      return r
    })
  }

  async function release(key, id) {
    if (!tracer) return _release(key, id)
    return tracer.span('lock.semaphore.release', { 'lock.key': key }, () => _release(key, id))
  }

  async function renew(key, id) {
    if (!tracer) return _renew(key, id)
    return tracer.span('lock.semaphore.renew', { 'lock.key': key }, () => _renew(key, id))
  }

  async function count(key) {
    await ensureConnected()
    const result = await client.eval(COUNT_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [String(ttlMs)],
    })
    return result
  }

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
