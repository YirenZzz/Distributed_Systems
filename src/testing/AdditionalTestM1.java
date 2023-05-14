package testing;

import org.junit.Test;


import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.net.ConnectException;
import java.net.UnknownHostException;
// import logger.newLoggerSetup;
import org.apache.log4j.Level;
import java.lang.Math;
import java.io.IOException;
import java.util.*;

public class AdditionalTestM1 extends TestCase {
	
	//Add at least 10 test cases of your choice that cover relevant system behavior not yet tested in the given test cases

    private KVStore kvClient;

	private Process ECSProcess;
    private List<Process> kvServerProcessList = new ArrayList<Process>();
    private List<KVStore> kvClientList = new ArrayList<KVStore>();

	/*
	 * Running this test file requires a ECS server at 60000, another at 60001
	 * and a KVServer at 50000
	 * 
	 */

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
	
	/*
	 * Test 1: put a key value after it was deleted
	 */
	@Test 
	public void testPutSameKey() {
		String key = "testPutSameKey";
		String value1 = "value1";
        String value2 = "value2";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value1);
            kvClient.put(key, "null");
			kvClient.put(key, value2);
            response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && 
			response.getValue().equals(value2));
	}
	
	// /*
	//  * Test 2: Test put for key that exceeds maximum length
	//  */
	@Test
    public void testPutTooLongKey(){

        String key = "";
        String value = "value";
        KVMessage response = null;
        Exception ex = null;

		for (int  i = 0; i < 30; i++){
			key = key + "k";
		}

        try{
            response = kvClient.put(key, value);
        }catch(Exception e){
            ex = e;
        }
		assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }

	// /*
	//  * Test 3: Test get for key that exceeds maximum length
	//  */
    @Test
    public void testGetTooLongKey(){

        String key = "";
        KVMessage response = null;
        Exception ex = null;

		for (int  i = 0; i < 30; i++){
			key = key + "k";
		}

        try{
            response = kvClient.get(key);
        }catch(Exception e){
            ex = e;
        }
		assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }


	// /*
	//  * Test 4: test put a value that exceeds maximum length
	//  */
    @Test
	public void testGetTooLongVal(){

        String key = "testGetTooLongVal";
		String value = "";
        KVMessage response = null;
        Exception ex = null;

		for (int  i = 0; i < 30; i++){
			key = key + "k";
		}

        try{
            response = kvClient.get(key);
        }catch(Exception e){
            ex = e;
        }
		assertTrue(ex == null && response.getStatus() == StatusType.FAILED);
    }

	// /*
	//  * Test 5: test put a value that should be saved in the long value file in the server,
	//  * longer than 4096 bytes
	//  */
    @Test
    public void testPutDelLongVal() {
		String key = "testPutLongVal";
		String value = "";
		KVMessage response1 = null, response2 = null;
		Exception ex = null;
		
		for (int i = 0; i<5000; i++) {
			value = value + "v";
		}

		try {
			response1 = kvClient.put(key, value);
			response2 = kvClient.put(key, null);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_SUCCESS && 
			response2.getStatus() == StatusType.DELETE_SUCCESS);
	}

	// /*
	//  * Test 6: test put and get a value that should be saved in the long value file in the server,
	//  * longer than 4096 bytes
	//  */
	@Test
	public void testPutGetLongVal() {
		String key = "testPutGetLongVal";
		String value = "";
		KVMessage response = null;
		Exception ex = null;
		
		for (int i = 0; i<5000; i++) {
			value = value + "v";
		}

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && 
			response.getValue().equals(value));
	}

	// /*
	//  * Test 7: test LRU cache eviction
	//  * 
	//  */
	@Test
	public void testLRU() {
		KVServer server = new KVServer("localhost", 50001, 3, "LRU", "localhost:50001", "localhost", 60001);//String name, String ECSHost, int ECSPort

		Exception ex = null;
		boolean res1=false, res2=false, res3=false, res4=false;

		try {
			server.putKV("testLRU", "value1");
			server.putKV("testLRU2", "value2");
			server.putKV("testLRU3", "value3");
			server.getKV("testLRU");
			server.getKV("testLRU2");
			server.putKV("testLRU4", "value4");

			res1 = server.inCache("testLRU");
			res2 = server.inCache("testLRU2");
			res3 = server.inCache("testLRU3");
			res4 = server.inCache("testLRU4"); //trigger cache evict
		} catch (Exception e) {
			ex = e;
		}

		System.out.println("ex: " + ex);
		System.out.println("res1: " + res1);

		server.close();

		assertTrue(ex == null && res1==true && res2==true && res3==false && res4==true);
	}

	// // /*
	// //  * Test 8: test LFU cache eviction
	// //  * 
	// //  */
	@Test
	public void testLFU() {
		KVServer server = new KVServer("localhost", 50001, 3, "LFU", "localhost:50001", "localhost", 60001);

		Exception ex = null;
		boolean res1=false, res2=false, res3=false, res4=false;

		try {
			server.putKV("testLFU", "value1");
			server.getKV("testLFU");
			server.putKV("testLFU2", "value2");
			server.getKV("testLFU2");
			server.putKV("testLFU3", "value3");
			server.putKV("testLFU4", "value4");
			res1 = server.inCache("testLFU");
			res2 = server.inCache("testLFU2");
			res3 = server.inCache("testLFU3");
			res4 = server.inCache("testLFU4");
		} catch (Exception e) {
			ex = e;
		}
		System.out.println("ex: " + ex);
		System.out.println("res1: " + res1);


		server.close();
		assertTrue(ex == null && res1==true && res2==true && res3==false && res4==true);
	}

	// // /*
	// //  * Test 9: test FIFO cache eviction
	// //  * 
	// //  */
	@Test
	public void testFIFO() {
		KVServer server = new KVServer("localhost", 50001, 3, "FIFO", "localhost:50001", "localhost", 60001);
		
		Exception ex = null;
		boolean res1=false, res2=false, res3=false, res4=false;

		try {
			server.putKV("testFIFO", "value1");
			server.putKV("testFIFO2", "value2");
			server.putKV("testFIFO3", "value3");
			server.putKV("testFIFO4", "value4");
			res1 = server.inCache("testFIFO");
			res2 = server.inCache("testFIFO2");
			res3 = server.inCache("testFIFO3");
			res4 = server.inCache("testFIFO4");
		} catch (Exception e) {
			ex = e;
		}

		server.close();
		assertTrue(ex == null && res1==false && res2==true && res3==true && res4==true);
	}

	// /*
	//  * Test 10: test the functionality of with no cache
	//  * 
	//  */
	@Test
	public void testNoCache() {
		KVServer server = new KVServer("localhost", 50001, 0, "None", "localhost:50001", "localhost", 60001);
		Exception ex = null;
		String res1=null, res2=null, res3=null, res4=null;

		try {
			server.putKV("testNoCache", "value1");
			server.putKV("testNoCache2", "value2");
			server.putKV("testNoCache3", "value3");
			server.putKV("testNoCache4", "value4");
			res1 = server.getKV("testNoCache");
			res2 = server.getKV("testNoCache2");
			res3 = server.getKV("testNoCache3");
			res4 = server.getKV("testNoCache4");
		} catch (Exception e) {
			ex = e;
		}

		server.close();
		assertTrue(ex == null && res1.equals("value1") && res2.equals("value2")  && res3.equals("value3")  && res4.equals("value4") );
	}

}