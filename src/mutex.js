import { createClient } from "redis"
import ms from "@prsm/ms"
import crypto from "node:crypto"

const RELEASE_SCRIPT = `if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`

export function mutex(options = {}) {
  const prefix = options.prefix ?? "lock:mutex:"
  const client = createClient(options.redis ?? {})
  let connectPromise = null

  async function ensureConnected() {
    if (!connectPromise) {
      connectPromise = client.connect()
    }
    await connectPromise
  }

  async function acquire(key, opts = {}) {
    await ensureConnected()
    const ttl = ms(opts.ttl ?? "10s")
    const id = opts.id ?? crypto.randomUUID()
    const result = await client.set(`${prefix}${key}`, id, { NX: true, PX: ttl })
    return { acquired: result === "OK", id: result === "OK" ? id : null }
  }

  async function release(key, id) {
    await ensureConnected()
    const result = await client.eval(RELEASE_SCRIPT, {
      keys: [`${prefix}${key}`],
      arguments: [id],
    })
    return result === 1
  }

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

  async function close() {
    if (connectPromise) {
      await connectPromise
      await client.quit()
      connectPromise = null
    }
  }

  client.on("error", () => {})

  return { acquire, release, peek, close }
}
