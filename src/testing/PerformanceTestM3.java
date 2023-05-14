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


public class PerformanceTestM3 extends TestCase {
	private final String ECSHOST = "localhost";
	private final int ECSPORT = 60000;
	private final int SERVERSTARTPORT = 40030;

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
        params.add("m3-ecs.jar");
        params.add("-a" + a);
		params.add("-p" + Integer.toString(p));
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
        params.add("m3-server.jar");
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
		numServers = 1;
		cacheSize = 100;

		System.out.println("Performance Test M3 Setup for " + numServers + "Servers and " + numClients + "Clients");
		dataset = new HashMap<String, String>();
		//set up dataset, assume using file name as key, file content as value
		File mailDir = new File("./maildir");
		
		//Index into the dataset folder
		for (File f : mailDir.listFiles()) { //ex. allen-p
			String personName = f.getName();
			String documentDir = f.getAbsolutePath() + "/all_documents";
			File documents = new File(documentDir);

			for (File curFile : documents.listFiles()) {
				String k = personName + "_" + curFile.getName();
				String content = null;
				//read file content
				try {
					BufferedReader reader = new BufferedReader(new FileReader(curFile.getAbsolutePath()));
					StringBuilder stringBuilder = new StringBuilder();
					String line = null;
					while ((line = reader.readLine()) != null) {
						stringBuilder.append(line);
						stringBuilder.append("\n");
					}
					//delete the last line separator
					stringBuilder.deleteCharAt(stringBuilder.length() - 1);
					reader.close();
					content = stringBuilder.toString();

				} catch (Exception e) {
					System.out.println("Failed to read content of " + curFile.getName() + ", " + e);		
				}

				//insert into dataset
				if (content != null ) {
					dataset.put(k, content);
				}
			}			
		}

		// view dataset
		// for (Map.Entry<String,String> entry : dataset.entrySet()) {
		// 	System.out.println("Key = " + entry.getKey() +
        //                      ", Value = " + entry.getValue());
		// 	break;
		// }
            
		//set up ECS
		//ecsClient = new ECSClient("localhost", ECSPORT);
		startECSProcess("localhost", ECSPORT);

		//set up servers
		for (int i = 0; i < numServers; i++) {
			System.out.println("Starting Server process");
			startServerProcess("FIFO", cacheSize, SERVERSTARTPORT+i);
		}

		//Wait for server processes to start, otherwise client connection will fail
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
			System.out.println("Thread sleep exception: " + e);
		}
		
		
		//set up clients
		for (int i = 0; i < numClients; i++) {
			KVStore curClient = new KVStore("localhost", SERVERSTARTPORT + i);
			try {
				curClient.connect();
				kvClientList.add(curClient);
			} catch (Exception e) {
				System.out.println("Client connection exception: " + e);
			}
		}

	}

	public void tearDown() {
		System.out.println("Performance Test M3 tearDown");
		for (KVStore client : kvClientList) {
			client.disconnect();
		}
		//todo: destroy client kvClient.disconnect();

		for (Process pc : kvServerProcessList) {
			System.out.println("Destroying server process");
			pc.destroy();
		}
		ECSProcess.destroy();
	}

    /*------ Performance Test -------*/
	/*
	 * Put 25% of keys, get all keys
	 * get-put ratio is 80-20
	 */
    @Test
	public void testPerformance() {
		System.out.println("Performance Test M3 function");

		//send client requests in round-robin manner
		KVMessage response = null;
		int total = dataset.size();
		int numPut = (int) (total / 4);
		int numGet = numPut; //todo: change back to totalNum

		//int times = 10;
		Exception ex = null;
		double put_percent = 0.8;
		double get_percent = 1- put_percent;
		int putBytes = 0;
		int getBytes = 0;
		long start = System.currentTimeMillis();

		//KVStore curClient = new KVStore("localhost", 40003);
		// put operations
		int curClientIdx = 0;
		int putCount = 0;
		for (String key : dataset.keySet()){
			if (putCount >= numPut) {
				break;
			}
			putCount++;

			try {
				KVStore curClient = kvClientList.get(curClientIdx);
				curClientIdx++;
				curClientIdx = curClientIdx % numClients;
				System.out.println("Putting_Key: " + key);
				String convertValue = dataset.get(key).replace("\r", "slash_r").replace("\n", "slash_n") + "\r\n";
				System.out.println("Putting_Val: " + convertValue);
				// curClient.put(key, dataset.get(key));
				curClient.put(key, convertValue);
				// putBytes += dataset.get(key).getBytes().length;
				// putBytes += dataset.get(key).replace("slash_r","\r").replace("slash_n","\n" ).getBytes().length;
				putBytes += convertValue.getBytes().length;
				// KVServer server = new
				// System.out.println("----cclientssss----: " + server.get(key).getValue());
				System.out.println("----cclientssss----: " + curClient.get(key).getValue());

			} catch (Exception e) {
				System.out.println("Client" + Integer.toString(curClientIdx) + "put exception: " + e);
			}	
			break;//todo: remove
		}

		// get operations
		curClientIdx = 0;
		int getCount = 0;

		for (String key : dataset.keySet()){
			if (getCount >= numGet) {
				break;
			}
			getCount++;
			// System.out.println("key" + key);

			try {
				KVStore curClient = kvClientList.get(curClientIdx);
				curClientIdx++;
				curClientIdx = curClientIdx % numClients;
				response = curClient.get(key);
				getBytes += response.getValue().getBytes().length;
				// System.out.println(response.getValue());

			} catch (Exception e) {
				System.out.println("Client" + Integer.toString(curClientIdx) + "put exception: " + e);
			}
			break;
		}

		long end = System.currentTimeMillis();
		
		double latency = (end - start);
		
		double totalBytes = 0;
		totalBytes = putBytes+getBytes;

		double throughput = (totalBytes/1000) / (latency * 1000);

		System.out.println("-------"+ Integer.toString(numServers) + " servers, " + Integer.toString(numClients) +" clients------");
		System.out.println("Latency : " + latency + "ms");
		System.out.println("Throughput : " + throughput + " KB/s");
		assertTrue(ex == null);
	}
}

