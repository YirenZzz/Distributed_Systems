package testing;

import org.junit.Test;

import app_kvECS.ECSClient;
import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.security.MessageDigest;

import java.net.ConnectException;
import java.net.UnknownHostException;
// import logger.newLoggerSetup;
import org.apache.log4j.Level;
import java.lang.Math;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import shared.HashRing;
import ecs.IECSNode;
import ecs.ECSNode;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.HandleStrResponse;

public class AdditionalTestM4 extends TestCase {
    private KVStore kvClient1;
    private KVStore kvClient2;
    private KVStore kvClient3;
    private KVStore kvClient4;
    private KVStore kvClient5;
    private ECSClient ecsClient;
    // private KVServer kvServer1;
    // private KVServer kvServer2;
    // private KVServer kvServer3;

    private int cacheSize;

    public void setUp() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        ecsClient = new ECSClient("localhost", 60000, lock);
        kvClient1 = new KVStore("localhost", 30000);
        kvClient2 = new KVStore("localhost", 30001);
        kvClient3 = new KVStore("localhost", 30002);
        kvClient4 = new KVStore("localhost", 30003);
        kvClient5 = new KVStore("localhost", 30004);

        try {
            kvClient1.connect();
            kvClient2.connect();
            kvClient3.connect();
            kvClient4.connect();
            kvClient5.connect();
        } catch (Exception e) {
            System.out.println("client connection error: " + e);
        }
        
    }

    public void tearDown() {
        System.out.println("Additional Test M4 tearDown");
        kvClient1.disconnect();
        kvClient2.disconnect();
        kvClient3.disconnect();
        kvClient4.disconnect();
        kvClient5.disconnect();
        ecsClient.shutdown();
    }

    private String getStrHash(String str){
        byte[] MD5Digest = null;
        try {
            byte[] nodeNameBytes = str.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            MD5Digest = md.digest(nodeNameBytes);
        } catch (Exception e) {
            System.out.println("getStrHash Exception" + e);
        }
        
        // Convert the hash to a hex string representation
        StringBuilder hexString = new StringBuilder();
        for (byte b : MD5Digest) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    
    /*
    * Test 1: Test BeginTX on server side
    */
    @Test
    public void testBeginTXAck() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{
            response = kvServer1.handleStrMsg("beginTX").responseMessage;
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        // System.out.println("response.getKeys()" + response.getKey());
        assertTrue(ex == null && response.getStatus() == StatusType.BEGINTX_SUCCESS && response.getKey().equals("beginTXack")==true);
    }
    
    /*
    * Test 2: Test BeginTX on client side
    */
    @Test
    public void testBeginTXsuccess() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{
            response = kvClient1.beginTX();
        } catch (Exception e){
            // System.out.println("Exception----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        // System.out.println("response status"+ response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.BEGINTX_SUCCESS);

    }

    /*
    * Test 3: Test if the tx ID is unique
    */
    @Test
    public void testUniqueTXID() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        String transactionId1 =null;
        String transactionId2 =null;
        
        try{
            // response = kvServer1.handleStrMsg("commitTX 1234568 put,lll,null;").responseMessage;
            response = kvClient1.beginTX();
            transactionId1 = response.getValue();

            response = kvClient1.beginTX();
            transactionId2 = response.getValue();

        } catch (Exception e){
            // System.out.println("Exception----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        // System.out.println("response status"+ response.getStatus());
        assertTrue(ex == null && transactionId1 !=null && transactionId1.equals(transactionId2)==false);
    }

    /*
    * Test 4: Test invalid argument in transaction log
    */
    @Test
    public void testTransactionError() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{
            response = kvServer1.handleStrMsg("commitTX 1234568 put;").responseMessage;
        } catch (Exception e){
            // System.out.println("Exception----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status"+ response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.TRANSACTION_ERROR);
    }
    
    /*
    * Test 5: Test getAllKeysSubQuery
    */
    @Test
    public void testGetAllKeysSubQuery() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{
            response = kvServer1.handleStrMsg("getAllKeysSubQuery").responseMessage;
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status"+ response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.GETALLKEYS_SUCCESS);
    }

    /*
    * Test 6: Test commit success
    */
    @Test
    public void testCommitSuccuss() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{

            kvClient1.put("k2","v2");
            response = kvClient1.beginTX();
            String transactionId = response.getValue();
            kvClient1.put("k2","v2");
            response = kvServer1.handleStrMsg("commitTX "+transactionId+" put,k2,v2;").responseMessage;
        } catch (Exception e){
            System.out.println("Exception----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        // System.out.println("response status"+ response.getStatus());
        // System.out.println("transactionId"+ response.getValue());
        assertTrue(ex == null && response.getStatus() == StatusType.COMMITTX_SUCCESS);
    }

    /*
    * Test 7: Test rollback
    */
    @Test
    public void testRollback() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;
        
        try{
            response = kvServer1.handleStrMsg("commitTX 12345 put,r3,r3;put,r4,r5").responseMessage;
        } catch (Exception e){
            System.out.println("Exception----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status"+ response.getStatus());
        // System.out.println("transactionId"+ response.getValue());
        assertTrue(ex == null && response.getStatus() == StatusType.ROLLED_BACK);
    }

    
    /*
    * Test 8: Test GetAllKeys Status
    */
    @Test
    public void testGetAllKeysStatus() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;

        try{
            response = kvServer1.handleStrMsg("getAllKeys").responseMessage;
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status: "+ response.getStatus());
        System.out.println("response keys: "+ response.getKey());
        assertTrue(ex == null && response.getStatus() == StatusType.GETALLKEYS_SUCCESS);
    }

    /*
    * Test 9: Test GetAllKeys Regex
    */
    @Test
    public void testGetAllKeysRegex() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;

        try{
            response = kvClient1.getAllKeys("k.*");
            // response = kvServer1.handleStrMsg("getAllKeys").responseMessage;
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status: "+ response.getStatus());
        System.out.println("response keys: "+ response.getKey());
        assertTrue(ex == null && response.getStatus() == StatusType.GETALLKEYS_SUCCESS && response.getKey().equals("k0,k3,k2,k1")==true);
    }

    /*
    * Test 10: Test GetAllKeys Regex with null key
    */
    @Test
    public void testGetAllKeysRegexNull() {
        KVServer kvServer1 = new KVServer("localhost",30000, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",30001, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",30002, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",30003, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",30004, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage response = null;

        try{
            response = kvClient1.getAllKeys("l.*");
            // response = kvServer1.handleStrMsg("getAllKeys").responseMessage;
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        System.out.println("response status: "+ response.getStatus());
        System.out.println("response keys: "+ response.getKey());
        assertTrue(ex == null && response.getStatus() == StatusType.GETALLKEYS_SUCCESS && response.getKey()==null);
    }
}