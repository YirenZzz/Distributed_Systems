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

public class AdditionalTestM3 extends TestCase {
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
        kvClient1 = new KVStore("localhost", 37923);
        kvClient2 = new KVStore("localhost", 44393);
        kvClient3 = new KVStore("localhost", 42937);
        kvClient4 = new KVStore("localhost", 37379);
        kvClient5 = new KVStore("localhost", 45745);
        // kvServer1 = new KVServer(40001, 10, "FIFO");
        // kvServer2 = new KVServer(40002, 10, "FIFO");
        // kvServer3 = new KVServer(40003, 10, "FIFO");
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
        System.out.println("Additional Test M3 tearDown");
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
    * Test 1: Test getPredecessor node
    */
    @Test
    public void testHashringGetPredecessor() {
        HashRing hashring = new HashRing();
        Exception ex = null;
        String result = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        IECSNode predecessor_of_node1 = null;

        hashring.insertNode(getStrHash("localhost:50000"), node1);
    
        try{
            predecessor_of_node1 = hashring.getPredecessor(getStrHash("localhost:50000"));
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && node1!=null && predecessor_of_node1== node1);
    }
    
    /*
    * Test 2: Test keyrange_read for one node
    */
    @Test
    public void testHashRingGetKeyRangeStrOneNode() {
        HashRing hashring = new HashRing();
        Exception ex = null;
        String result = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        
        hashring.insertNode(getStrHash("localhost:50000"), node1);

        try{
            result = hashring.getHashRingReadStr();
            System.out.println("aaaaa"+ result);
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && result.equals("2b786438d2c6425dc30de0077ea6494e,2b786438d2c6425dc30de0077ea6494d,localhost:50000;"));

    }

    /*
    * Test 3: Test send put request to replicates
    */
    public void testPutReplicas() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            kvClient2.put("aaa", "bbb"); //44393(42937,37379); 2 (3,4)
            response_server1 = kvServer1.handleStrMsg("put aaa bbb").responseMessage;
            response_server2 = kvServer2.handleStrMsg("put aaa bbb").responseMessage;
            response_server3 = kvServer3.handleStrMsg("put aaa bbb").responseMessage;
            response_server4 = kvServer4.handleStrMsg("put aaa bbb").responseMessage;
            response_server5 = kvServer5.handleStrMsg("put aaa bbb").responseMessage;
            // System.out.println("response_server1 status"+ response_server1.getStatus());
            // System.out.println("response_server2 status"+ response_server2.getStatus());
            // System.out.println("response_server3 status"+ response_server3.getStatus());
            // System.out.println("response_server4 status"+ response_server4.getStatus());
            // System.out.println("response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        
        assertTrue(ex == null 
        && response_server1.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && (response_server2.getStatus() == StatusType.PUT_UPDATE || response_server2.getStatus() ==StatusType.PUT_SUCCESS)
        && response_server3.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server4.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server5.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);

    }
    
    /*
    * Test 4: Test send get from replicates
    */
    public void testGetReplicas() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            kvClient2.put("aaa", "bbb"); //44393(42937,37379); 2 (3,4)
            // kvServer2.handleStrMsg("put aaa bbb");
            // System.out.println("puT????");
            // put_response = kvServer2.handleStrMsg("put aaa bbb").responseMessage;
            response_server1 = kvServer1.handleStrMsg("get aaa").responseMessage;
            response_server2 = kvServer2.handleStrMsg("get aaa").responseMessage;
            response_server3 = kvServer3.handleStrMsg("get aaa").responseMessage;
            response_server4 = kvServer4.handleStrMsg("get aaa").responseMessage;
            response_server5 = kvServer5.handleStrMsg("get aaa").responseMessage;

            System.out.println("get response_server1 status"+ response_server1.getStatus());
            System.out.println("get response_server2 status"+ response_server2.getStatus());
            System.out.println("get response_server3 status"+ response_server3.getStatus());
            System.out.println("get response_server4 status"+ response_server4.getStatus());
            System.out.println("get response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception---0-" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        //todo: check the expected answer
        assertTrue(ex == null 
        && response_server1.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server2.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE
        && response_server3.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE
        && response_server4.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE
        && response_server5.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
        // assertTrue(ex == null && result.equals("2b786438d2c6425dc30de0077ea6494e,2b786438d2c6425dc30de0077ea6494d,localhost:50000;"));

    }

    /*
    * Test 5: Test keyrange_read from server side
    */
    public void testKeyRangeRead() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            response_server1 = kvServer1.handleStrMsg("keyrange_read").responseMessage;

            System.out.println("response_server1 status: "+ response_server1.getStatus());
            System.out.println("response_server1 key: "+ response_server1.getKey());

        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        //todo: check the expected answer
        assertTrue(ex == null 
        && response_server1.getStatus() == StatusType.KEYRANGE_READ_SUCCESS
        && response_server1.getKey().equals("7273a35184c6afea24472ba04c37fc4e,ed8e5ccfc9085b39c5e56ff7e56bd2a,127.0.0.1:37923;c32999258ca3ee488e53b7f0ad0c8090,59eeff400d990cd4248d0ff74b2150c4,127.0.0.1:44393;e60188496372db3124defe4eb355621b,7273a35184c6afea24472ba04c37fc4d,127.0.0.1:42937;ed8e5ccfc9085b39c5e56ff7e56bd2b,c32999258ca3ee488e53b7f0ad0c808f,127.0.0.1:37379;59eeff400d990cd4248d0ff74b2150c5,e60188496372db3124defe4eb355621a,127.0.0.1:45745;"));
        // assertTrue(ex == null && result.equals("2b786438d2c6425dc30de0077ea6494e,2b786438d2c6425dc30de0077ea6494d,localhost:50000;"));

    }
    

    /*
    * Test 6: Test if the coordinator and replicas servers are correct when put key and values
    */
    public void testCoordinatorReplicas() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;

        try{
            // kvClient2.put("ccc", "ddd"); //44393(42937,37379); 2 (3,4)
            result = kvServer4.handleStrMsg("put ddd eee").nextInstParam;
            
            System.out.println("result: "+ result);
            // System.out.println("response_server2 status"+ response_server2.getStatus());
            // System.out.println("response_server3 status"+ response_server3.getStatus());
            // System.out.println("response_server4 status"+ response_server4.getStatus());
            // System.out.println("response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        
        assertTrue(ex == null 
        && result.equals("127.0.0.1:45745,127.0.0.1:37923,127.0.0.1:37379;ddd eee"));

    }

    /*
    * Test 7: Test using coordinator delete key
    */
    public void testCoordinatorDelete() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            // kvClient2.put("aaa", "bbb"); //44393(42937,37379); 2 (3,4)
            response_server1 = kvServer2.handleStrMsg("put aaa bbb").responseMessage;
            response_server2 = kvServer2.handleStrMsg("put aaa").responseMessage;
            response_server3 = kvServer3.handleStrMsg("put aaa").responseMessage;
            response_server4 = kvServer4.handleStrMsg("put aaa").responseMessage;
            // response_server5 = kvServer5.handleStrMsg("put aaa").responseMessage;
            // System.out.println("response_server1 status"+ response_server1.getStatus());
            System.out.println("response_server2 status"+ response_server2.getStatus());
            System.out.println("response_server3 status"+ response_server3.getStatus());
            System.out.println("response_server4 status"+ response_server4.getStatus());
            // System.out.println("response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        
        assertTrue(ex == null 
        && (response_server2.getStatus() == StatusType.DELETE_SUCCESS)
        && response_server3.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server4.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);

    }

    /*
    * Test 8: Test using replicates delete key
    */
    public void testReplicasDelete() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            // kvClient2.put("aaa", "bbb"); //44393(42937,37379); 2 (3,4)
            response_server1 = kvServer2.handleStrMsg("put aaa bbb").responseMessage;
            // response_server2 = kvServer2.handleStrMsg("put aaa").responseMessage;
            response_server3 = kvServer3.handleStrMsg("put aaa").responseMessage;
            response_server4 = kvServer4.handleStrMsg("put aaa").responseMessage;
            // response_server5 = kvServer5.handleStrMsg("put aaa").responseMessage;
            System.out.println("response_server1 status"+ response_server1.getStatus());
            // System.out.println("response_server2 status"+ response_server2.getStatus());
            System.out.println("response_server3 status"+ response_server3.getStatus());
            System.out.println("response_server4 status"+ response_server4.getStatus());
            // System.out.println("response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        
        assertTrue(ex == null 
        && response_server3.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server4.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);

    }

     /*
    * Test 9: Test shutdown nodes from ECS side
    */
    public void testShutdownNode() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            response_server1 = ecsClient.handleStrMsg("shutdownnode localhost:37923").responseMessage;
            String str = kvServer2.metadata;
            // kvServer1.handleStrMsg("keyrange").responseMessage;
            // kvServer1.handleStrMsg("NODESHUDOWN_SUCCESS");
            // System.out.println("response_server2 string: "+ str);
            // System.out.println("response_server2 status: "+ response_server2.getStatus());
            // System.out.println("response_server2 aaaa keyrange: "+ response_server2.getKey());
        } catch (Exception e){
            System.out.println("Exception-----" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        //todo: check the expected answer
        assertTrue(ex == null 
        && response_server1.getStatus() == StatusType.NODESHUDOWN_SUCCESS
        && response_server1.getKey().equals("shutdownnodedirectly"));

    }
    /*
    * Test 10: Test forward put request command
    */
    public void testPutReplicasCommand() {
        KVServer kvServer1 = new KVServer("localhost",37923, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer2 = new KVServer("localhost",44393, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer3 = new KVServer("localhost",42937, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer4 = new KVServer("localhost",37379, 10, "FIFO", "local_server", "localhost",60000);
        KVServer kvServer5 = new KVServer("localhost",45745, 10, "FIFO", "local_server", "localhost",60000);


        Exception ex = null;
        String result = null;
        KVMessage put_response = null;
        KVMessage response_server1 = null;
        KVMessage response_server2 = null;
        KVMessage response_server3 = null;
        KVMessage response_server4 = null;
        KVMessage response_server5 = null;

        try{
            kvClient2.put("aaa", "bbb"); //44393(42937,37379); 2 (3,4)
            // kvServer2.handleStrMsg("put aaa bbb");
            kvServer3.handleStrMsg("putreplica aaa bbb");
            kvServer4.handleStrMsg("putreplica aaa bbb");
            // System.out.println("puT????");
            // put_response = kvServer2.handleStrMsg("put aaa bbb").responseMessage;
            response_server1 = kvServer1.handleStrMsg("get aaa").responseMessage;
            response_server2 = kvServer2.handleStrMsg("get aaa").responseMessage;
            response_server3 = kvServer3.handleStrMsg("get aaa").responseMessage;
            response_server4 = kvServer4.handleStrMsg("get aaa").responseMessage;
            response_server5 = kvServer5.handleStrMsg("get aaa").responseMessage;

            System.out.println("get response_server1 status"+ response_server1.getStatus());
            System.out.println("get response_server2 status"+ response_server2.getStatus());
            System.out.println("get response_server3 status"+ response_server3.getStatus());
            System.out.println("get response_server4 status"+ response_server4.getStatus());
            System.out.println("get response_server5 status"+ response_server5.getStatus());

        } catch (Exception e){
            System.out.println("Exception---" + e);
            ex = e;
        }
        kvServer1.close();
        kvServer2.close();
        kvServer3.close();
        kvServer4.close();
        kvServer5.close();
        //todo: check the expected answer
        assertTrue(ex == null 
        && response_server1.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE
        && response_server2.getStatus() == StatusType.GET_SUCCESS
        && response_server3.getStatus() == StatusType.GET_SUCCESS
        && response_server4.getStatus() == StatusType.GET_SUCCESS
        && response_server5.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
        // assertTrue(ex == null && result.equals("2b786438d2c6425dc30de0077ea6494e,2b786438d2c6425dc30de0077ea6494d,localhost:50000;"));

    }
}