import { describe, it, expect, beforeEach, afterAll } from "vitest"
import { mutex } from "../src/index.js"
import { createClient } from "redis"

let redis
let lock

beforeEach(async () => {
  if (!redis) {
    redis = createClient()
    await redis.connect()
  }
  await redis.flushDb()

  if (lock) await lock.close().catch(() => {})
  lock = mutex()
})

afterAll(async () => {
  await lock?.close().catch(() => {})
  await redis?.quit().catch(() => {})
})

describe("mutex", () => {
  it("acquires and releases", async () => {
    const { acquired, id } = await lock.acquire("test-key")
    expect(acquired).toBe(true)
    expect(id).toBeTruthy()

    const released = await lock.release("test-key", id)
    expect(released).toBe(true)
  })

  it("is exclusive", async () => {
    const r1 = await lock.acquire("exclusive")
    expect(r1.acquired).toBe(true)

    const lock2 = mutex()
    const r2 = await lock2.acquire("exclusive")
    expect(r2.acquired).toBe(false)
    expect(r2.id).toBe(null)

    await lock2.close()
  })

  it("allows re-acquire after release", async () => {
    const r1 = await lock.acquire("reacquire")
    await lock.release("reacquire", r1.id)

    const r2 = await lock.acquire("reacquire")
    expect(r2.acquired).toBe(true)
  })

  it("auto-expires after TTL", async () => {
    const r1 = await lock.acquire("ttl-test", { ttl: 50 })
    expect(r1.acquired).toBe(true)

    await new Promise(r => setTimeout(r, 100))

    const lock2 = mutex()
    const r2 = await lock2.acquire("ttl-test")
    expect(r2.acquired).toBe(true)
    await lock2.close()
  })

  it("rejects release with wrong id", async () => {
    const r1 = await lock.acquire("wrong-id")
    expect(r1.acquired).toBe(true)

    const released = await lock.release("wrong-id", "not-the-right-id")
    expect(released).toBe(false)

    const lock2 = mutex()
    const r2 = await lock2.acquire("wrong-id")
    expect(r2.acquired).toBe(false)
    await lock2.close()
  })

  it("release after expiry is safe", async () => {
    const r1 = await lock.acquire("expiry-safe", { ttl: 50 })
    await new Promise(r => setTimeout(r, 100))

    const lock2 = mutex()
    const r2 = await lock2.acquire("expiry-safe")
    expect(r2.acquired).toBe(true)

    const released = await lock.release("expiry-safe", r1.id)
    expect(released).toBe(false)

    const info = await lock2.peek("expiry-safe")
    expect(info.held).toBe(true)
    expect(info.holder).toBe(r2.id)

    await lock2.close()
  })

  it("supports custom id", async () => {
    const { acquired, id } = await lock.acquire("custom-id", { id: "my-instance" })
    expect(acquired).toBe(true)
    expect(id).toBe("my-instance")

    const released = await lock.release("custom-id", "my-instance")
    expect(released).toBe(true)
  })

  it("peek returns correct state when held", async () => {
    const r1 = await lock.acquire("peek-held", { ttl: "5s" })
    const info = await lock.peek("peek-held")
    expect(info.held).toBe(true)
    expect(info.holder).toBe(r1.id)
    expect(info.ttl).toBeGreaterThan(0)
    expect(info.ttl).toBeLessThanOrEqual(5000)
  })

  it("peek returns correct state when not held", async () => {
    const info = await lock.peek("peek-not-held")
    expect(info.held).toBe(false)
    expect(info.holder).toBe(null)
    expect(info.ttl).toBe(-1)
  })

  it("different prefixes don't interfere", async () => {
    const lockA = mutex({ prefix: "a:" })
    const lockB = mutex({ prefix: "b:" })

    const r1 = await lockA.acquire("shared-name")
    const r2 = await lockB.acquire("shared-name")

    expect(r1.acquired).toBe(true)
    expect(r2.acquired).toBe(true)

    await lockA.close()
    await lockB.close()
  })

  it("concurrent operations before first connect", async () => {
    const fresh = mutex()
    const results = await Promise.all([
      fresh.acquire("concurrent-a"),
      fresh.acquire("concurrent-b"),
      fresh.acquire("concurrent-c"),
    ])
    expect(results.every(r => r.acquired)).toBe(true)
    expect(new Set(results.map(r => r.id)).size).toBe(3)
    await fresh.close()
  })

  it("uses default TTL of 10s", async () => {
    const { acquired } = await lock.acquire("default-ttl")
    expect(acquired).toBe(true)

    const info = await lock.peek("default-ttl")
    expect(info.ttl).toBeGreaterThan(9000)
    expect(info.ttl).toBeLessThanOrEqual(10000)
  })
})
