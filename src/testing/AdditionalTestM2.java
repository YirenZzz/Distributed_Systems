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


public class AdditionalTestM2 extends TestCase {
    private KVStore kvClient1;
    private KVStore kvClient2;
    private KVStore kvClient3;
    private ECSClient ecsClient;
    // private KVServer kvServer1;
    // private KVServer kvServer2;
    // private KVServer kvServer3;

    private int cacheSize;

    public void setUp() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        ecsClient = new ECSClient("localhost", 60000, lock);
        kvClient1 = new KVStore("localhost", 40001);
        kvClient2 = new KVStore("localhost", 40002);
        kvClient3 = new KVStore("localhost", 40003);
        // kvServer1 = new KVServer(40001, 10, "FIFO");
        // kvServer2 = new KVServer(40002, 10, "FIFO");
        // kvServer3 = new KVServer(40003, 10, "FIFO");
        try {
   kvClient1.connect();
            kvClient2.connect();
            kvClient3.connect();
  } catch (Exception e) {
  }
        
    }

    public void tearDown() {
        System.out.println("Additional Test M2 tearDown");
        kvClient1.disconnect();
        kvClient2.disconnect();
        kvClient3.disconnect();

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
    * Test 1: add node hashring
    */
    @Test
    public void testHashringAddNode() {
        HashRing hashring = new HashRing();
        Exception ex = null;
        String result = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        
        hashring.insertNode(getStrHash("localhost:50000"), node1);

        try{
            result = hashring.getHashRingStr();
            // System.out.println("aaaaa"+ result);
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && result.equals("localhost:50000,57781912184744560596689028710406703438,57781912184744560596689028710406703437,localhost:50000;"));
    }

    /*
    * Test 2: get successor node hashring
    */
    @Test
    public void testHashringGetSuccessor() {
        HashRing hashring = new HashRing();
        Exception ex = null;
        String result = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        IECSNode successor_of_node1 = null;

        hashring.insertNode(getStrHash("localhost:50000"), node1);
    
        try{
            successor_of_node1 = hashring.getSuccessor(getStrHash("localhost:50000"));
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && node1!=null && successor_of_node1== node1);
    }

    /*
    * Test 3: Update hashring
    */
    @Test
    public void testHashringUpdate() {
        
        HashRing hashring = new HashRing();
        Exception ex = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        IECSNode node2 = new ECSNode("localhost:50001", "localhost", 50001);
        IECSNode successor_of_node1 = null;
        IECSNode successor_of_node2 = null ;

        hashring.insertNode(getStrHash("localhost:50000"), node1);
        hashring.insertNode(getStrHash("localhost:50001"), node2);

        try{
            successor_of_node1 = hashring.getSuccessor(getStrHash("localhost:50000"));
            successor_of_node2 = hashring.getSuccessor(getStrHash("localhost:50001"));        //    System.out.println("bbbbb");
        } catch (Exception e){
                System.out.println("Exception" + e);
                ex = e;
        }
        
        assertTrue(ex == null && node2 != null & node1 != null && successor_of_node1 == node2 && successor_of_node2 == node1);
    }

    /*
    * Test 4: Add duplicate node to hashring
    */
    @Test
    public void testHashringAddDupNode() {
        
        HashRing hashring = new HashRing();
        Exception ex = null;
        int expected_size = 3;

        hashring.insertNode(getStrHash("localhost:30000"), new ECSNode("localhost:30000", "localhost", 30000));
        hashring.insertNode(getStrHash("localhost:30001"), new ECSNode("localhost:30001", "localhost", 30001));
        hashring.insertNode(getStrHash("localhost:30002"), new ECSNode("localhost:30002", "localhost", 30002));

        try{
            hashring.insertNode(getStrHash("localhost:30000"), new ECSNode("localhost:30000", "localhost", 30000));
            // System.out.println("aaaaa"+ hashring.nodeMap.size());
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && hashring.nodeMap.size() == expected_size);
    }

    /*
    * Test 5: Remove node from hashring
    */

    @Test
    public void testHashringRemoveNode() {
        
        HashRing hashring = new HashRing();
        Exception ex = null;
        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        IECSNode node2 = new ECSNode("localhost:50001", "localhost", 50001);
        IECSNode successor_of_node1 = null;
        IECSNode successor_of_node2 = null ;

        hashring.insertNode(getStrHash("localhost:50000"), node1);
        hashring.insertNode(getStrHash("localhost:50001"), node2);

        try{
            hashring.deleteNode(getStrHash("localhost:50001"));
        } catch (Exception e){
                System.out.println("Exception" + e);
                ex = e;
        }

        assertTrue(ex == null && node1!=null && hashring.getSuccessorKey(getStrHash("localhost:50000")).equals(getStrHash("localhost:50000")));
    }

    /*
    * Test 6: Remove non-existing node from hashring
    */

    @Test
    public void testHashringRemoveNonExistNode() {
        
        HashRing hashring = new HashRing();
        Exception ex = null;
        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        IECSNode node2 = new ECSNode("localhost:50001", "localhost", 50001);
        IECSNode successor_of_node1 = null;
        IECSNode successor_of_node2 = null ;

        hashring.insertNode(getStrHash("localhost:50000"), node1);
        hashring.insertNode(getStrHash("localhost:50001"), node2);

        try{
            hashring.deleteNode(getStrHash("localhost:50003"));
        } catch (Exception e){
                System.out.println("Exception" + e);
                ex = e;
        }
        assertNull(ex);
    }


    /*
    * Test 7: add 1 node and check metadata for ECS that initially is empty
    */
    @Test
    public void testECSMetadaupdate1() {
        // KVServer kvServer1 = new KVServer(40001, 10, "FIFO");
        HandleStrResponse response = null;
        Exception ex = null;
        String expected_metadata = "localhost:40001,190459318794675105163907833856048063468,190459318794675105163907833856048063467,localhost:40001;";
        
        try{
            System.out.println("ecsClient.hashRing"+ecsClient.hashRing.getHashRingStr()); //empty

            ecsClient.handleStrMsg("addnode localhost:40001"); //hash ring contains server1
            ecsClient.handleStrMsg("Metadataupdate "+ecsClient.hashRing.getHashRingStr());

        }catch(Exception e){
            ex = e;
        }

        assertTrue(ex == null && ecsClient.hashRing.getHashRingStr().equals(expected_metadata));
    }

    /*
    * Test 8: shutdown 1 node and check metadata for ECS that initially is empty
    */
    @Test
    public void testECSshutdownnode() {
        // KVServer kvServer1 = new KVServer(40001, 10, "FIFO");
        HandleStrResponse response = null;
        Exception ex = null;
        String expected_metadata = "";
        
        try{
            System.out.println("ecsClient.hashRing"+ecsClient.hashRing.getHashRingStr()); //empty
            ecsClient.handleStrMsg("shutdownnode localhost:40001"); //hash ring contains server1
            ecsClient.handleStrMsg("Metadataupdate "+ecsClient.hashRing.getHashRingStr());

        }catch(Exception e){
            ex = e;
        }

        assertTrue(ex == null && ecsClient.hashRing.getHashRingStr().equals(""));
    }

    /*
    * Test 9: add 2 node and check metadata for ECS that initially is empty
    */
    @Test
    public void testECSaddNodes() {
        // KVServer kvServer1 = new KVServer(40001, 10, "FIFO");
        HandleStrResponse response = null;
        Exception ex = null;
        String expected_metadata = "localhost:40002,190459318794675105163907833856048063468,6058323019276838330023197426228815441,localhost:40002;localhost:40001,6058323019276838330023197426228815442,190459318794675105163907833856048063467,localhost:40001;";
        
        try{
            ecsClient.handleStrMsg("addnode localhost:40001"); //hash ring contains server1
            ecsClient.handleStrMsg("addnode localhost:40002"); //hash ring contains server1
            ecsClient.handleStrMsg("Metadataupdate "+ecsClient.hashRing.getHashRingStr());
            // System.out.println("ecsClient.hashRing"+ecsClient.hashRing.getHashRingStr()); //empty

        }catch(Exception e){
            ex = e;
        }

        assertTrue(ex == null && ecsClient.hashRing.getHashRingStr().equals(expected_metadata));
    }

    /*
    * Test 10: check keyrange hashring
    */
    @Test
    public void testHashRingGetKeyRangeStr() {
        HashRing hashring = new HashRing();
        Exception ex = null;
        String result = null;

        IECSNode node1 = new ECSNode("localhost:50000", "localhost", 50000);
        
        hashring.insertNode(getStrHash("localhost:50000"), node1);

        try{
            result = hashring.getKeyRangeStr();
            System.out.println("aaaaa"+ result);
        } catch (Exception e){
            System.out.println("Exception" + e);
            ex = e;
        }
        assertTrue(ex == null && result.equals("57781912184744560596689028710406703438,57781912184744560596689028710406703437,localhost:50000;"));
    }

}