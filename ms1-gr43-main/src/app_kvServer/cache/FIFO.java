package app_kvServer.cache;

import java.util.concurrent.ConcurrentHashMap; //cache table structure
import java.util.concurrent.ConcurrentLinkedDeque; //FIFO queue
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import logger.LogSetup;

public class FIFO implements CacheInterface {

	//cache structure
	ConcurrentHashMap<String, String> hashMap;
	//FIFO Deque
	ConcurrentLinkedDeque<String> hashEntryDeque;

	int totalCacheSize;
	int remainingCacheSize;
	private ReentrantLock remainingCacheSizeLock;

	private Logger logger;

	public FIFO(int cacheSize) {
		hashMap = new ConcurrentHashMap<>();
		hashEntryDeque = new ConcurrentLinkedDeque<>();
		remainingCacheSizeLock = new ReentrantLock();

		totalCacheSize = cacheSize;
		remainingCacheSize = totalCacheSize;	
	}

	/*
	 * Returns value of a given key
	 */
	public String getCache(String key) {
		return hashMap.get(key);
	}

	/*
	 * Returns error code
	 */
	public void putCache(String key, String value) {
		remainingCacheSizeLock.lock();

		if (remainingCacheSize == 0) {
			remainingCacheSizeLock.unlock();
			evictCache();

		} else {
			remainingCacheSize--;
			remainingCacheSizeLock.unlock();
		}

		hashMap.put(key, value);
		hashEntryDeque.addLast(key);
	}


	/*
	 * Remove a cache entry by key
	 */
	public void removeCache(String key) {
		remainingCacheSizeLock.lock();
		remainingCacheSize++;
		remainingCacheSizeLock.unlock();
		
		hashMap.remove(key);
		hashEntryDeque.remove(key);
	}
	

	/*
	 * Delete entire cache structure
	 */
	public void clearCache() {
		hashMap.clear();
	}

	/*
	 * FIFO
	 */
	private void evictCache() {
		String k = hashEntryDeque.removeFirst();
		if (k == null) {
			logger.error("got null from first element of hashEntryDeque");
		} else {
			hashMap.remove(k);
		}
		
	}

	// for testing
	// public static void main(String[] args) {
	// System.out.println("Testing Cache");
	// CacheDB c = new CacheDB();
	// System.out.println(c.getCache("k1"));
	//
	// c.putCache("k1", "v1");
	// c.putCache("k2", "v2");
	//
	// System.out.println(c.getCache("k1"));
	//
	// c.clearCache();
	// System.out.println(c.getCache("k1"));
	//
	// }
}
