/**
 * Simple in-memory cache with TTL support for SSR result caching.
 * Reduces BigQuery queries during peak traffic by serving cached responses.
 */

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

const cache = new Map<string, CacheEntry<any>>();

/**
 * Retrieve cached value if it exists and hasn't expired.
 * @param key Cache key
 * @returns Cached data or null if missing/expired
 */
export function getCached<T>(key: string): T | null {
  const entry = cache.get(key);
  if (!entry) return null;

  if (Date.now() > entry.expiresAt) {
    cache.delete(key);
    return null;
  }

  return entry.data as T;
}

/**
 * Store value in cache with TTL.
 * @param key Cache key
 * @param data Data to cache
 * @param ttlMinutes Time-to-live in minutes (default 360 = 6 hours; data refreshes weekly so long TTL is safe)
 */
export function setCached<T>(key: string, data: T, ttlMinutes = 360): void {
  cache.set(key, {
    data,
    expiresAt: Date.now() + ttlMinutes * 60 * 1000,
  });
}

/**
 * Clear cache entry by key.
 * @param key Cache key to delete
 */
export function clearCache(key: string): void {
  cache.delete(key);
}

/**
 * Clear all cache entries.
 */
export function clearAllCache(): void {
  cache.clear();
}
