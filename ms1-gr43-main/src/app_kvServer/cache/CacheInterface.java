package app_kvServer.cache;

public interface CacheInterface {

	public String getCache(String key);
	public void putCache(String key, String value);
	public void removeCache(String key); //remove a cache entry by key
	public void clearCache();

}