package testing;

import org.junit.Test;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;


public class PerformanceTestSubscribe extends TestCase {
	private final String ECSHOST = "localhost";
	private final int ECSPORT = 60000;
	private final int SERVERSTARTPORT = 50000;

	private int numServers;
	private int numClients;
	private ECSClient ecsClient;
	private Process ECSProcess;
	private List<Process> kvServerProcessList = new ArrayList<Process>();
    private List<KVStore> kvClientList = new ArrayList<KVStore>();
	private HashMap<String, String> dataset;

	private int cacheSize;
	
	//helper functions
	private void startECSProcess(String a, int p) {
		List<String> params = new ArrayList<String>();
        params.add("java");
        params.add("-jar");
        params.add("m4-ecs.jar");
		ProcessBuilder pb = new ProcessBuilder(params);
        //pb.redirectErrorStream(true);
		try {
			ECSProcess = pb.start();

		} catch (Exception e) {
			System.out.println("ECS Exception" + e);
		}
	}

	private void startServerProcess(String cacheStrategy, int cacheSize, int serverPort) {
		List<String> params = new ArrayList<String>();
        params.add("java");
        params.add("-jar");
        params.add("m4-server.jar");
        params.add("-p" + Integer.toString(serverPort));
		params.add("-s" + cacheStrategy);
		params.add("-c" + Integer.toString(cacheSize));
		ProcessBuilder pb = new ProcessBuilder(params);

        pb.redirectErrorStream(true);
		try {
			Process pc = pb.start();
			kvServerProcessList.add(pc);
		} catch (Exception e) {
			System.out.println("Server Exception" + e);
		}

	}

	public void setUp() {
		numClients = 1;
		numServers = 5;
		cacheSize = 100;

		System.out.println("Performance Test Subscribe Setup for " + numServers + "Servers and " + numClients + "Clients");
		//set up dataset, assume using file name as key, file content as value
		// startECSProcess("localhost", ECSPORT);

		// //set up servers
		// for (int i = 0; i < numServers; i++) {
		// 	System.out.println("Starting Server process");
		// 	startServerProcess("FIFO", cacheSize, SERVERSTARTPORT+i);
		// }

		// //Wait for server processes to start, otherwise client connection will fail
		// try {
		// 	Thread.sleep(1000);
		// } catch (Exception e) {
		// 	System.out.println("Thread sleep exception: " + e);
		// }
		
		
		//set up clients
		for (int i = 0; i < numClients; i++) {
			KVStore curClient = new KVStore("localhost", SERVERSTARTPORT + i);
			try {
				curClient.connect();
				kvClientList.add(curClient);
			} catch (Exception e) {
				int curPort = SERVERSTARTPORT + i;
				System.out.println("Client connection exception: " + Integer.toString(curPort) + e);
			}
		}

	}

	public void tearDown() {
		System.out.println("Performance Test M4 Subscribe tearDown");
		for (KVStore client : kvClientList) {
			client.disconnect();
		}
		//todo: destroy client kvClient.disconnect();

		for (Process pc : kvServerProcessList) {
			System.out.println("Destroying server process");
			pc.destroy();
		}
		// ECSProcess.destroy();
	}

    /*------ Performance Test -------*/
	/*
	 * Put 25% of keys, get all keys
	 * get-put ratio is 80-20
	 */
    @Test
	public void testPerformance() {
		System.out.println("Performance Test Subscribe function");
		
		long start = System.currentTimeMillis();
		Exception ex = null;
		try{
			for (int i = 0; i < 10; i++) {
				for (KVStore client : kvClientList) {
					client.beginTX();
					//client.unsubscribeKey("a");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			//System.out.println(e.printStackTrace());
			System.out.println("exception: "+ e.getMessage());
			ex = e;
		}

		long end = System.currentTimeMillis();
		
		double latency = (end - start);
		latency = latency / 10;
		
		System.out.println("-------"+ Integer.toString(numServers) + " servers, " + Integer.toString(numClients) +" clients------");
		System.out.println("Latency: " + latency + "ms");
		assertTrue(ex == null);
	}
}