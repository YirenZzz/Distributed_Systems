package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
		IKVClient client = new KVClient();
		return client;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
		//String host, int port, int cacheSize, String strategy, String name, String ECSHost, int ECSPort
		String serverName = "localhost:" + Integer.toString(port);
		IKVServer server = new KVServer("localhost", port, cacheSize, strategy, serverName, "localhost", 60000);
		return server;
	}
}
