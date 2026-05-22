export interface RedisOptions {
  url?: string
  host?: string
  port?: number
  password?: string
}

export interface MutexOptions {
  redis?: RedisOptions
  prefix?: string
}

export interface AcquireOptions {
  ttl?: number | string
  id?: string
}

export interface AcquireResult {
  acquired: boolean
  id: string | null
}

export interface PeekResult {
  held: boolean
  holder: string | null
  ttl: number
}

export interface MutexLockInfo {
  key: string
  holder: string
  ttl: number
}

export interface Mutex {
  acquire(key: string, options?: AcquireOptions): Promise<AcquireResult>
  release(key: string, id: string): Promise<boolean>
  peek(key: string): Promise<PeekResult>
  list(): Promise<MutexLockInfo[]>
  close(): Promise<void>
}

export function mutex(options?: MutexOptions): Mutex

export interface SemaphoreOptions {
  max: number
  ttl?: number | string
  redis?: RedisOptions
  prefix?: string
}

export interface SemaphoreAcquireOptions {
  id?: string
}

export interface SemaphorePeekResult {
  active: number
  max: number
  available: number
  holders: string[]
}

export interface SemaphoreLockInfo {
  key: string
  active: number
  max: number
  available: number
  holders: string[]
}

export interface Semaphore {
  acquire(key: string, options?: SemaphoreAcquireOptions): Promise<AcquireResult>
  release(key: string, id: string): Promise<true>
  renew(key: string, id: string): Promise<boolean>
  count(key: string): Promise<number>
  peek(key: string): Promise<SemaphorePeekResult>
  list(): Promise<SemaphoreLockInfo[]>
  close(): Promise<void>
}

export function semaphore(options: SemaphoreOptions): Semaphore
