import { describe, it, expect, beforeEach, afterAll } from "vitest"
import { semaphore } from "../src/index.js"
import { createClient } from "redis"

let redis
let sem

beforeEach(async () => {
  if (!redis) {
    redis = createClient()
    await redis.connect()
  }
  await redis.flushDb()

  if (sem) await sem.close().catch(() => {})
  sem = semaphore({ max: 3, ttl: "60s" })
})

afterAll(async () => {
  await sem?.close().catch(() => {})
  await redis?.quit().catch(() => {})
})

describe("semaphore", () => {
  it("throws if max is not positive", () => {
    expect(() => semaphore({ max: 0 })).toThrow("max must be a positive number")
    expect(() => semaphore({ max: -1 })).toThrow("max must be a positive number")
  })

  it("acquires up to max", async () => {
    const r1 = await sem.acquire("slots")
    const r2 = await sem.acquire("slots")
    const r3 = await sem.acquire("slots")
    expect(r1.acquired).toBe(true)
    expect(r2.acquired).toBe(true)
    expect(r3.acquired).toBe(true)

    const r4 = await sem.acquire("slots")
    expect(r4.acquired).toBe(false)
    expect(r4.id).toBe(null)
  })

  it("release frees a slot", async () => {
    const ids = []
    for (let i = 0; i < 3; i++) {
      const r = await sem.acquire("release-test")
      ids.push(r.id)
    }

    const r4 = await sem.acquire("release-test")
    expect(r4.acquired).toBe(false)

    await sem.release("release-test", ids[0])

    const r5 = await sem.acquire("release-test")
    expect(r5.acquired).toBe(true)
  })

  it("TTL expiry frees slots", async () => {
    const shortSem = semaphore({ max: 1, ttl: 50 })

    const r1 = await shortSem.acquire("ttl-test")
    expect(r1.acquired).toBe(true)

    await new Promise(r => setTimeout(r, 100))

    const r2 = await shortSem.acquire("ttl-test")
    expect(r2.acquired).toBe(true)

    await shortSem.close()
  })

  it("renew extends lease", async () => {
    const shortSem = semaphore({ max: 1, ttl: 100 })

    const r1 = await shortSem.acquire("renew-test")
    expect(r1.acquired).toBe(true)

    await new Promise(r => setTimeout(r, 60))
    const renewed = await shortSem.renew("renew-test", r1.id)
    expect(renewed).toBe(true)

    await new Promise(r => setTimeout(r, 60))

    const shortSem2 = semaphore({ max: 1, ttl: 100 })
    const r2 = await shortSem2.acquire("renew-test")
    expect(r2.acquired).toBe(false)

    await shortSem.close()
    await shortSem2.close()
  })

  it("renew on expired lease returns false", async () => {
    const shortSem = semaphore({ max: 1, ttl: 50 })

    const r1 = await shortSem.acquire("renew-expired")
    await new Promise(r => setTimeout(r, 200))

    const renewed = await shortSem.renew("renew-expired", r1.id)
    expect(renewed).toBe(false)

    await shortSem.close()
  })

  it("count returns correct active count", async () => {
    const r1 = await sem.acquire("count-test")
    const r2 = await sem.acquire("count-test")

    const c1 = await sem.count("count-test")
    expect(c1).toBe(2)

    await sem.release("count-test", r1.id)

    const c2 = await sem.count("count-test")
    expect(c2).toBe(1)
  })

  it("peek returns correct state", async () => {
    const r1 = await sem.acquire("peek-test")
    const r2 = await sem.acquire("peek-test")

    const info = await sem.peek("peek-test")
    expect(info.active).toBe(2)
    expect(info.max).toBe(3)
    expect(info.available).toBe(1)
    expect(info.holders).toContain(r1.id)
    expect(info.holders).toContain(r2.id)
  })

  it("peek on empty semaphore", async () => {
    const info = await sem.peek("empty")
    expect(info.active).toBe(0)
    expect(info.max).toBe(3)
    expect(info.available).toBe(3)
    expect(info.holders).toEqual([])
  })

  it("concurrent acquire from multiple instances", async () => {
    const sem1 = semaphore({ max: 2, ttl: "60s" })
    const sem2 = semaphore({ max: 2, ttl: "60s" })

    const r1 = await sem1.acquire("multi")
    const r2 = await sem2.acquire("multi")
    expect(r1.acquired).toBe(true)
    expect(r2.acquired).toBe(true)

    const r3 = await sem1.acquire("multi")
    expect(r3.acquired).toBe(false)

    const r4 = await sem2.acquire("multi")
    expect(r4.acquired).toBe(false)

    await sem1.close()
    await sem2.close()
  })

  it("concurrent operations before first connect", async () => {
    const fresh = semaphore({ max: 5, ttl: "60s" })
    const results = await Promise.all([
      fresh.acquire("concurrent"),
      fresh.acquire("concurrent"),
      fresh.acquire("concurrent"),
    ])
    expect(results.every(r => r.acquired)).toBe(true)
    expect(new Set(results.map(r => r.id)).size).toBe(3)

    const c = await fresh.count("concurrent")
    expect(c).toBe(3)
    await fresh.close()
  })

  it("heartbeat keeps lease alive across TTL windows", async () => {
    const shortSem = semaphore({ max: 1, ttl: 100 })

    const r1 = await shortSem.acquire("heartbeat-test")
    expect(r1.acquired).toBe(true)

    for (let i = 0; i < 3; i++) {
      await new Promise(r => setTimeout(r, 60))
      const ok = await shortSem.renew("heartbeat-test", r1.id)
      expect(ok).toBe(true)
    }

    const shortSem2 = semaphore({ max: 1, ttl: 100 })
    const r2 = await shortSem2.acquire("heartbeat-test")
    expect(r2.acquired).toBe(false)

    await shortSem.close()
    await shortSem2.close()
  })
})
