package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.util.*;

public class PerformanceTestM1 extends TestCase {
    private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}

    /*------ Performance Test -------*/
    @Test
	public void test80Put20Get() {
		KVMessage response = null;
		int total = 1000;
		int times = 10;
		Exception ex = null;
		double put_percent = 0.8;
		double get_percent = 1- put_percent;
		int putBytes = 0;
		int getBytes = 0;

		List<Integer> key_list = new ArrayList<Integer>();

		for (int i = 1000; i < 2000; i++){
			key_list.add(i);
		}
		for (int i = 0; i < total; i++){
			try {
				kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
			} catch (Exception e) {
				ex = e;
			}
		}
		// System.out.println("List : " + key_list.toString());

		long start = System.currentTimeMillis();
		for (int k = 0; k < times; k++){
			for (int i = 0; i < total * put_percent; i++){
				try {
					kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
					putBytes += ('v' + key_list.get(i).toString()).getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
			
			for (int i = 0; i < total * get_percent; i++){
				try {
					response = kvClient.get(key_list.get(i).toString());
					getBytes += response.getValue().getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
		}
		long end = System.currentTimeMillis();

		double latency = (end - start);
		
		double totalBytes = 0;
		// totalBytes = (5 * total * put_percent + total * get_percent * 5) / times;
		totalBytes = putBytes+getBytes;

		double throughput = (totalBytes/1000) / (latency * 1000);

		System.out.println("-------"+ put_percent*100 +"% put, " + get_percent*100 +"% get------");
		System.out.println("Latency : " + latency + "ms");
		System.out.println("Throughput : " + throughput + " KB/s");
		assertTrue(ex == null);
	}

	@Test
	public void test50Put50Get() {
		KVMessage response = null;
		int total = 1000;
		int times = 10;
		Exception ex = null;
		double put_percent = 0.5;
		double get_percent = 1- put_percent;
		int putBytes = 0;
		int getBytes = 0;

		List<Integer> key_list = new ArrayList<Integer>();

		for (int i = 1000; i < 2000; i++){
			key_list.add(i);
		}
		for (int i = 0; i < total; i++){
			try {
				kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
			} catch (Exception e) {
				ex = e;
			}
		}
		// System.out.println("List : " + key_list.toString());

		long start = System.currentTimeMillis();
		for (int k = 0; k < times; k++){
			for (int i = 0; i < total * put_percent; i++){
				try {
					kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
					putBytes += ('v' + key_list.get(i).toString()).getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
			
			for (int i = 0; i < total * get_percent; i++){
				try {
					response = kvClient.get(key_list.get(i).toString());
					getBytes += response.getValue().getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
		}
		long end = System.currentTimeMillis();

		double latency = (end - start);
		
		double totalBytes = 0;
		// totalBytes = (5 * total * put_percent + total * get_percent * 5) / times;
		totalBytes = putBytes+getBytes;

		double throughput = (totalBytes/1000) / (latency * 1000);

		System.out.println("-------"+ put_percent*100 +"% put, " + get_percent*100 +"% get------");
		System.out.println("Latency : " + latency + "ms");
		System.out.println("Throughput : " + throughput + " KB/s");
		assertTrue(ex == null);
	}

	@Test
	public void test20Put80Get() {
		KVMessage response = null;
		int total = 1000;
		int times = 10;
		Exception ex = null;
		double put_percent = 0.2;
		double get_percent = 1- put_percent;
		int putBytes = 0;
		int getBytes = 0;

		List<Integer> key_list = new ArrayList<Integer>();

		for (int i = 1000; i < 2000; i++){
			key_list.add(i);
		}
		for (int i = 0; i < total; i++){
			try {
				kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
			} catch (Exception e) {
				ex = e;
			}
		}
		// System.out.println("List : " + key_list.toString());

		long start = System.currentTimeMillis();
		for (int k = 0; k < times; k++){
			for (int i = 0; i < total * put_percent; i++){
				try {
					kvClient.put(key_list.get(i).toString(), 'v' + key_list.get(i).toString());
					putBytes += ('v' + key_list.get(i).toString()).getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
			
			for (int i = 0; i < total * get_percent; i++){
				try {
					response = kvClient.get(key_list.get(i).toString());
					getBytes += response.getValue().getBytes().length;
				} catch (Exception e) {
					ex = e;
				}
			}
		}
		long end = System.currentTimeMillis();

		double latency = (end - start);
		
		double totalBytes = 0;
		// totalBytes = (5 * total * put_percent + total * get_percent * 5) / times;
		totalBytes = putBytes+getBytes;

		double throughput = (totalBytes/1000) / (latency * 1000);

		System.out.println("-------"+ put_percent*100 +"% put, " + get_percent*100 +"% get------");
		System.out.println("Latency : " + latency + "ms");
		System.out.println("Throughput : " + throughput + " KB/s");
		assertTrue(ex == null);
	}

	
}