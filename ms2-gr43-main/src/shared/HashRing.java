package shared;

import java.util.TreeMap;
import java.math.BigInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.*;

import ecs.IECSNode;
import ecs.ECSNode;



public class HashRing {
    private static Logger logger = Logger.getRootLogger();
    public TreeMap<String, IECSNode> nodeMap; //key is 32-bit hex
    private ReentrantReadWriteLock lock;

    public HashRing(){
        this.nodeMap = new TreeMap<>(); 
        this.lock = new ReentrantReadWriteLock();
    }

    /*
     * Construct HashRing from String
     * <node_name>,<range_from>,<range_to>,<ip:port>;...
     */
    public HashRing(String data){
        this.nodeMap = new TreeMap<>(); 
        this.lock = new ReentrantReadWriteLock();

        String[] nodeDataList = data.split(";");
        for (int i=0; i<nodeDataList.length; i++) {
            String[] nodeData = nodeDataList[i].split(",");

            String nodeName;
            String nodeAddr;
            int lowHashIdx;
            if (nodeData.length == 4) { //NodeName, LowHash, HighHash, NodeAddr
                nodeName = nodeData[0];
                nodeAddr = nodeData[3];
                lowHashIdx = 1;
            } else { //LowHash, HighHash, NodeAddr
                nodeName = nodeData[2];
                nodeAddr = nodeData[2];
                lowHashIdx = 0;
            }

            String[] arrNodeAddr = nodeAddr.split(":");
            IECSNode curNode = new ECSNode(nodeName, arrNodeAddr[0], Integer.valueOf(arrNodeAddr[1]));
            curNode.setNodeHashRange(nodeData[lowHashIdx], nodeData[lowHashIdx+1]);

            try {
                String key = getHashVal(nodeAddr); //Hashing <IP>:<Port>
                insertNode(key, curNode);

            } catch (Exception e) {
                logger.error("Error creating node for " + nodeDataList[i] + "Exception: " + e);
            }
        }
    }

    /*
     * Get the MD5 hash of a string
     */
    public String getHashVal(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] MD5Digest = md.digest(str.getBytes("UTF-8"));

            // Convert the hash to a hex string representation
            StringBuilder hexString = new StringBuilder();
            for (byte b : MD5Digest) {
                hexString.append(String.format("%02x", b));
            }
            String res = hexString.toString();
            return res;

        } catch (Exception e) {
            logger.error("Unable to compute hash of " + str + ": " + str);
            return null;
        }
    }

    /*
     * Get IECSNode of a key 
     * Can be used to get the server respoonsible for a key
     */
    public IECSNode getSuccessorInclusive(String key) {
        this.lock.readLock().lock();
        // Hash ring is empty
        if (this.nodeMap.size() == 0) {
            this.lock.readLock().unlock();
            return null;
        }

        String successorKey = null;
        int cmp = new BigInteger(key, 16).compareTo(new BigInteger(nodeMap.lastKey(), 16));
        if (cmp <= 0) { //key <= nodeMap.lastKey()
            //Key managed by server is inclusive of current node, exclusive of predecessor node
            if (nodeMap.containsKey(key)){
                successorKey = key;
            } else {
                successorKey = nodeMap.higherKey(key);
            }
        } else {
            successorKey = nodeMap.firstKey();
        }

        IECSNode res = nodeMap.get(successorKey);
        this.lock.readLock().unlock();
        return res; 
    }

    /*
     * Get the key of the successor 
     */
    public String getSuccessorKey(String key) {
        this.lock.readLock().lock();
        // Hash ring is empty
        if (this.nodeMap.size() == 0) {
            this.lock.readLock().unlock();
            return null;
        }

        //Only one elemets, which must be the successor
        if (this.nodeMap.size() == 1) {
            for (Map.Entry<String,IECSNode> entry : this.nodeMap.entrySet()) {
                this.lock.readLock().unlock();
                return entry.getKey();
            }
        }

        String successorKey = null;
        int cmp = new BigInteger(key, 16).compareTo(new BigInteger(nodeMap.lastKey(), 16));
        if (cmp < 0) { //key < nodeMap.lastKey() (If the key is equal to the last node key, then the successor is the first node)
            successorKey = nodeMap.higherKey(key);
        } else {
            successorKey = nodeMap.firstKey();
        }
        this.lock.readLock().unlock();
        return successorKey;
    }

    /*
     * Get the successor of a key on the hash ring
     * A helper function
     */
    public IECSNode getSuccessor(String key) {
        String SuccessorKey = this.getSuccessorKey(key);
        if (SuccessorKey == null) {
            return null;
        }
        return nodeMap.get(SuccessorKey);
    }

    /*
     * Insert a node, and modify hash ranges 
     */
    public void insertNode(String key, IECSNode node) {
        this.lock.writeLock().lock();
        if (nodeMap.size() == 0) {
            BigInteger intKey = new BigInteger(key, 16);
            BigInteger lowHash = intKey.add(BigInteger.valueOf(1));
            node.setNodeHashRange(lowHash.toString(16), intKey.toString(16));
        } else {
            IECSNode successorNode = getSuccessor(key);
            String[] successorHash = successorNode.getNodeHashRange();
            BigInteger intCurKey = new BigInteger(key, 16);
            BigInteger intSuccLowHash = intCurKey.add(BigInteger.valueOf(1));
            successorNode.setNodeHashRange(intSuccLowHash.toString(16), successorHash[1]);
            node.setNodeHashRange(successorHash[0], intCurKey.toString(16));
        }

        nodeMap.put(key, node);
        this.lock.writeLock().unlock();
    }
    
    /*
     * Delete a node, and change predecessor's hash ranges 
     */
    public void deleteNode(String key) {
        this.lock.writeLock().lock();
        if (nodeMap.size() == 0 || nodeMap.get(key) == null) {
            this.lock.writeLock().unlock();
            return;
        }

        //Return directly if the node does not exist
        IECSNode curNode = nodeMap.get(key);
        if (curNode == null) {
            this.lock.writeLock().unlock();
            return;
        }

        //modify successor hash range
        String[] curNodeHash = curNode.getNodeHashRange();
        IECSNode successorNode = getSuccessor(key);
        String[] successorNodeHash = successorNode.getNodeHashRange();
        successorNode.setNodeHashRange(curNodeHash[0], successorNodeHash[1]);

        //update nodeMap
        nodeMap.replace(getSuccessorKey(key), successorNode);
        nodeMap.remove(key);
        this.lock.writeLock().unlock();
    }

    /*
     * Convert hash ring into string of key range
     * <range_from>,<range_to>,<ip:port>;... 
     * Used in Keyrange command of client
     */
    public String getKeyRangeStr() {
        String result = "";
        for (Map.Entry<String, IECSNode> entry : nodeMap.entrySet()) {
            IECSNode curNode = entry.getValue();
            String[] hashRange = curNode.getNodeHashRange();
            result = result + hashRange[0] + "," + hashRange[1] + "," + curNode.getNodeHost() + ":" + curNode.getNodePort() + ";";
        }
        return result;
    }

    /*
     * Convert hash ring into string
     * <node_name>,<range_from>,<range_to>,<ip:port>;...
     * Used in Storing metadata
     */
    public String getHashRingStr() {
        String result = "";
        for (Map.Entry<String, IECSNode> entry : nodeMap.entrySet()) {
            IECSNode curNode = entry.getValue();
            String[] hashRange = curNode.getNodeHashRange();
            result = result + curNode.getNodeName() + "," + hashRange[0] + "," + hashRange[1] + "," + curNode.getNodeHost() + ":" + curNode.getNodePort() + ";";
        }
        return result;
    }

    /*
     * Convert hash ring to metadata
     * <ip:port>,<hashRange>;...
     */
    //? need to check format
    // public Map<String, String[]> getMetaData() {
    //     Map<String, String[]> metadata = new HashMap<String, String[]>();

    //     for (Map.Entry<String, IECSNode> entry : nodeMap.entrySet()) {
    //         IECSNode curNode = entry.getValue();
    //         String[] hashRange = curNode.getNodeHashRange();
    //         String servername = curNode.getNodeHost() + ":" + curNode.getNodePort();
    //         metadata.put(servername, hashRange);
    //     }
    //     return metadata;
    // }

}
