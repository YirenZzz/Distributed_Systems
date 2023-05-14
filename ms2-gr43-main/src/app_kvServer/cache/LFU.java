package app_kvServer.cache;

import java.util.concurrent.ConcurrentHashMap; //cache table structure
import java.util.concurrent.ConcurrentLinkedDeque; //LFU queue
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import logger.LogSetup;

public class LFU implements CacheInterface {

	//cache structure
	ConcurrentHashMap<String, String> hashMap;
	// cache with the same frequency
	ConcurrentHashMap<Integer, ConcurrentLinkedDeque<String>> frequency; 
	// # of visit times
	ConcurrentHashMap<String, Integer> visits; 

	int frequency_key = -1;
	int totalCacheSize;
	int remainingCacheSize;
	private ReentrantLock remainingCacheSizeLock;

	private Logger logger;

	public LFU(int cacheSize) {
		hashMap = new ConcurrentHashMap<>();
		remainingCacheSizeLock = new ReentrantLock();
		frequency = new ConcurrentHashMap<>();
		frequency.put(1, new ConcurrentLinkedDeque<String>());
		visits = new ConcurrentHashMap<>();
		totalCacheSize = cacheSize;
		remainingCacheSize = totalCacheSize;	
	}

	/*
	 * Returns value of a given key
	 * case 1: if no cache, return null
	 * case 2: if have cache, return cache, and frequency+1 & visit + 1
	 */
	public String getCache(String key) {
		if (hashMap.containsKey(key)){
			int count = visits.get(key);
			visits.put(key, count+1);
			frequency.get(count).remove(key);
			
			if (frequency.get(count).size() ==0){
				if (count ==frequency_key){
					frequency_key++;
				}
				frequency.remove(count);
			}

			if (!frequency.containsKey(count+1)){
				frequency.put(count+1, new ConcurrentLinkedDeque<String>());
			}
			frequency.get(count+1).add(key);
		}	
		return hashMap.get(key);
	}

	/*
	 * Put:
	 * case 1. if remainingCacheSize = 0, delete the least frequent key
	 * case 2. if remainingCacheSize !=0 & key exits, update cache, visit+1
	 * case 3. if remainingCacheSize !=0 & key not exits, add cache, visit+1
	 */
	public void putCache(String key, String value) {
		remainingCacheSizeLock.lock();
		// case 2
		Integer count = 1;
		if (hashMap.containsKey(key)){
			hashMap.put(key, value);
			getCache(key);
			return;
		}

		// case 1
		if (remainingCacheSize == 0) {
			remainingCacheSizeLock.unlock();
			evictCache();
		}else{
			remainingCacheSize--;
			remainingCacheSizeLock.unlock();
		}
		
		// case 3
		hashMap.put(key, value);
		visits.put(key, count);
		if (!frequency.containsKey(count)){
			frequency.put(count, new ConcurrentLinkedDeque<String>() );
		}
		frequency.get(count).add(key);
		frequency_key = 1;		
	}


	/*
	 * Remove a cache entry by key
	 */
	public void removeCache(String key) {
		if (hashMap.containsKey(key)){
			hashMap.remove(key);
			int count = visits.get(key);
			visits.remove(key);
			frequency.get(count).remove(key);
			remainingCacheSizeLock.lock();
			remainingCacheSize++;
			remainingCacheSizeLock.unlock();

			if (frequency.get(count).size() ==0){
				if (count ==frequency_key){
					frequency_key++;
				}
				frequency.remove(count);
			}

		}
	}
	

	/*
	 * Delete entire cache structure
	 */
	public void clearCache() {
		hashMap.clear();
		visits.clear();
		frequency.clear();
		frequency_key = -1;
	}

	/*
	 * LFU
	 */
	private void evictCache() {
		// System.out.println(frequency.get(frequency_key).getFirst());
		String k = frequency.get(frequency_key).getFirst();
		if (k == null) {
			logger.error("got null");
		} else {
			hashMap.remove(k);
			visits.remove(k);
			frequency.get(frequency_key).remove(k);
		}
		
	}
	

}
