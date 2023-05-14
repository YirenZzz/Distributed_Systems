package app_kvServer.cache;

import java.util.concurrent.ConcurrentHashMap; //cache table structure
import java.util.concurrent.ConcurrentLinkedDeque; //LRU queue
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.*;

import org.apache.log4j.Logger;
import logger.LogSetup;


public class LRU implements CacheInterface {

	//cache structure
	ConcurrentHashMap<String, String> hashMap;
	//LRU Deque
	ConcurrentLinkedDeque<String> hashEntryDeque;
	List<String> history;

	int totalCacheSize;
	int remainingCacheSize;
	private ReentrantLock remainingCacheSizeLock;

	private Logger logger;

	public LRU(int cacheSize) {
		hashMap = new ConcurrentHashMap<>();
		history = new CopyOnWriteArrayList<>();
		hashEntryDeque = new ConcurrentLinkedDeque<>();
		remainingCacheSizeLock = new ReentrantLock();

		totalCacheSize = cacheSize;
		remainingCacheSize = totalCacheSize;	
	}

	/*
	 * Returns value of a given key
	 */
	public String getCache(String key) {
		if (hashMap.containsKey(key)){
			history.remove(history.indexOf(key));
			history.add(key);
		}

		return hashMap.get(key);
	}

	/*
	 * Returns error code
	 */
	public void putCache(String key, String value) {
		remainingCacheSizeLock.lock();

		if (hashMap.containsKey(key)){
			history.remove(history.indexOf(key));
			history.add(key);
		}
		else{
			history.add(key);
		}
		
		hashMap.put(key, value);

		if (remainingCacheSize == 0) {
			remainingCacheSizeLock.unlock();
			evictCache();

		} else {
			remainingCacheSize--;
			remainingCacheSizeLock.unlock();
		}
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
	 * LRU
	 */
	private void evictCache() {
		String k = history.get(0);
		if (k == null) {
			logger.error("got null from first element of hashEntryDeque");
		} else {
			hashMap.remove(k);
			history.remove(0);
		}
	}

}
