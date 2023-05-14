package app_kvECS;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque; 
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Iterator; 
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.w3c.dom.Text;

import shared.HashRing;
import shared.communication.*;
import shared.messages.TextMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.HandleStrResponse;

import ecs.IECSNode;
import ecs.ECSNode;
import ecs.FailureDetector;
import logger.LogSetup;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ECSClient implements IECSClient, IServerObject{

    private static Logger logger = Logger.getRootLogger();
	public static final String PROMPT = "ECS> ";
    public static final String SERVER_ROOT = "/Servers";
    public static final String METADATA_PATH = "/Metadata";
    private static final String CONFIGFILENAME = "serverConfig.properties";

    private static String zkServerDir;
    private ZooKeeper zk;
    private ServerSocket serverSocket;
    private boolean running;

    public HashRing hashRing; 
    private ArrayList<Thread> connections;
    private String targetServerName;
    private ReentrantReadWriteLock configFileLock;

    /*
	 * Sends metadataupdate messages
	 * metadataupdate <metadata>
	 */
	private class MetaDataUpdateSender implements Runnable {
		private String rcvNodeIP; 
		private int rcvNodePort;
        private String metaData;

		/**
		 * @param rcvNodeAddr	receiver node ip
		 * @param rcvNodePort 	receiver node port
         * @param metaData      meta data
		 */
		public MetaDataUpdateSender(String rcvNodeIP, int rcvNodePort, String metaData) {
			this.rcvNodeIP = rcvNodeIP;
			this.rcvNodePort = rcvNodePort;
            this.metaData = metaData;
		}

		public void run() {
			CommClient cl = new CommClient(rcvNodeIP, rcvNodePort);
			
            try{
                cl.connect();
                cl.sendMessage(new TextMessage("metadataupdate " + metaData));
                TextMessage respMsg  = cl.receiveMessage();
                logger.info("ECS metadataupdate received from host "+rcvNodeIP+", port"+ rcvNodePort + ": "+respMsg.getMsg().trim());
                cl.disconnect();

			} catch (Exception e) {
				logger.error ("MetaDataUpdateSender failed to send message to node " + rcvNodePort + ": " + e);
			}
		}
	}

    /*
     * Start ECS service
     */
    public ECSClient(String addr, int port, ReentrantReadWriteLock cl) {
        connections = new ArrayList<Thread>();
        this.hashRing = new HashRing();
        this.configFileLock = cl;
        
        //Initialize ECS server
        logger.info("Initializing ECS Server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("ECS Server listening on port: " 
            		+ serverSocket.getLocalPort());  
					
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
        }
        logger.info("Initialized ECS Server");
    }

    @Override
    public boolean start() {
        return false;
    }
    @Override
    public boolean stop() {
        this.running = false;
        return false;
    }

    private boolean isRunning() {
        return this.running;
    }

    /*
     * Consistently listen to socket
     */
    public void run(){
        this.running = true;
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                CommServer connection = 
	                		new CommServer(client, this);
	                
					Thread client_thread = new Thread(connection);
					client_thread.start();
					connections.add(client_thread);
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
	}

    @Override
    public boolean shutdown() {
        logger.info("ECS client shutting down");
        this.running = false;
        return true;
    }

    /*
     * handle new node adder from server
     */
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return null;
    }
    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }
    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        return false;
    }

    /**
     * Get a map of all nodes (node name -> ECSNode)
     */
    @Override
    public Map<String, IECSNode> getNodes() {
        return null;
    }
    @Override
    public IECSNode getNodeByKey(String key) {
        return null;
    }

    private String getStrHash(String str){
        byte[] MD5Digest = null;
        try {
            byte[] nodeNameBytes = str.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            MD5Digest = md.digest(nodeNameBytes);
        } catch (Exception e) {
            logger.error("Failed to convert "+str+" to MD5: " + e);
        }
        
        // Convert the hash to a hex string representation
		StringBuilder hexString = new StringBuilder();
        for (byte b : MD5Digest) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    /*
     * Add server to the metadata
     */
    private void addServerNode(String host, int port) {
        String nodeName = host + ":" + Integer.toString(port);
        IECSNode newNode = new ECSNode(nodeName, host, port);
        String key = getStrHash(nodeName);
        logger.info("adding server node with key: " + key);
        hashRing.insertNode(key, newNode);
    }

    /*
     * handle command from KVServer
     */
    @Override
    public HandleStrResponse handleStrMsg(String strMsg){
        logger.info("ECSClient received: " + strMsg);

        String[] tokens = strMsg.trim().split("\\s+");
        if (tokens[0].equals("addnode")) {
			this.targetServerName = tokens[1];
            String[] targetServerInfo = targetServerName.split(":");
            addServerNode(targetServerInfo[0], Integer.parseInt(targetServerInfo[1])); //host, port

            //write new node information to file 
            configFileLock.writeLock().lock();
            try {      
                Properties prop = new Properties();
                prop.load(new FileInputStream(CONFIGFILENAME));
                prop.setProperty(targetServerName, "connected");
                prop.store(new FileOutputStream(CONFIGFILENAME), "updated by ECSClient addnode");
                configFileLock.writeLock().unlock();
            } catch (IOException e) {
                logger.error("Failed to write into server config file: " + e);
                configFileLock.writeLock().unlock();
            }

            KVMessage resp = new KVMessageObject(KVMessage.StatusType.ADDNODEACK_SUCCESS, "addnodeack", hashRing.getHashRingStr());
            
            //get successor in metadata
            IECSNode successorNode = hashRing.getSuccessor(getStrHash(targetServerName));
            if (successorNode.getNodePort() != Integer.parseInt(targetServerInfo[1])) { //Successor is a distinct node, todo: host is different?
                String successorNodeAddr = successorNode.getNodeHost()+":"+successorNode.getNodePort();
                String strParam = successorNodeAddr + ";" + this.targetServerName;
                return new HandleStrResponse(NextInst.SUCCESSORSERVERWRITELOCK, resp, strParam); //the next instruction is to send write lock to successor
            }

            //successor node is the same as the current server node
            return new HandleStrResponse(NextInst.NOINST, resp); //the next instruction is to send write lock to successor 
		} 

        else if (tokens[0].equals("shutdownnode")) {
            this.targetServerName = tokens[1];

            //update the configuration file
            try {
                configFileLock.writeLock().lock();
                Properties prop = new Properties();
                prop.load(new FileInputStream(CONFIGFILENAME));
                prop.remove(targetServerName);
                prop.store(new FileOutputStream(CONFIGFILENAME), "updated by ECSClient shutdownnode");
                configFileLock.writeLock().unlock();

            } catch (IOException e) {
                logger.error("shutdownnode failed to write into server config file: " + e);
            }

            this.hashRing.deleteNode(this.hashRing.getHashVal(tokens[1]));
          
            if (this.hashRing.nodeMap.size() == 0) {
                KVMessage resp = new KVMessageObject(KVMessage.StatusType.NODESHUDOWN_SUCCESS, "shutdownnodedirectly", null);
                return new HandleStrResponse(NextInst.NOINST, resp); 
            }

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.NODESHUDOWN_SUCCESS, "shutdownnodeack", null);
            return new HandleStrResponse(NextInst.SHUTDOWNNODE, resp, tokens[1]); 

		} else if (tokens[0].equals("nodeshutdowndetected")) { //resonstruct the storage service after a node crashes
            logger.info("ECS nodeshutdowndetected received: " + tokens[1]);
            //check whether the node initiated shutdown
            IECSNode shutDownNode = this.hashRing.nodeMap.get(hashRing.getHashVal(tokens[1]));
            if (shutDownNode == null) {
                logger.warn("nodeshutdowndetected the node does not exist");
                KVMessage resp = new KVMessageObject(KVMessage.StatusType.NODESHUTDOWNDETECTED_ACK, null, null);
                return new HandleStrResponse(NextInst.NOINST, resp);
            }

            //update metadata
            this.hashRing.deleteNode(hashRing.getHashVal(tokens[1]));

            //if no replica is left, return directly
            if (hashRing.nodeMap.size() <= 1) {
                logger.info("hash ring size is trivial");
                KVMessage resp = new KVMessageObject(KVMessage.StatusType.NODESHUTDOWNDETECTED_ACK, null, null);
                return new HandleStrResponse(NextInst.NOINST, resp);
            }

            //send metadata update to all servers
            for (Map.Entry<String, IECSNode> entry : this.hashRing.nodeMap.entrySet()) {
                IECSNode curNode = entry.getValue();
                MetaDataUpdateSender sender = new MetaDataUpdateSender(curNode.getNodeHost(), curNode.getNodePort(), this.hashRing.getHashRingStr());
                Thread updateSender = new Thread(sender);
                updateSender.start();
            }

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.NODESHUTDOWNDETECTED_ACK, null, null);
			return new HandleStrResponse(NextInst.SHUTDOWNDETECTEDADDREPLICA, resp, tokens[1]);

		} else {
			logger.warn("Unknown command at ECS: " + strMsg.trim());
            KVMessage resp = new KVMessageObject(KVMessage.StatusType.CONNECTED, "ECS response", null);
            return new HandleStrResponse(NextInst.NOINST, resp);
		}
    }

     /**
     * Subsequent instructions
     * @return  cache size
     */
    public void handleNextInst(NextInst inst, String strMsg) {
        logger.info("Instruction is " +inst.name() + ", Instruction parameter is " + strMsg); //<successor_ip>:<successor_port>;<target_ip>:<target_port>
        if (inst == NextInst.SUCCESSORSERVERWRITELOCK) {
            //send write lock instruction
            String successorAddr = strMsg.split(";")[0];
            CommClient successorServer = new CommClient(successorAddr.split(":")[0], Integer.parseInt(successorAddr.split(":")[1]));
            String strResp = null;
            try {
                successorServer.connect();
                successorServer.sendMessage(new TextMessage("writelock " + this.hashRing.getHashRingStr()+ " " + strMsg));
                TextMessage resp = successorServer.receiveMessage();
                strResp = resp.getMsg().trim();

            } catch (Exception e) {
                logger.error("Error sending message in SUCCESSORSERVERWRITELOCK to "+successorAddr+" :[" + e +"]"); 
                return; 
            }

            //Send transferdatainvoke to successor server //todo: not needed?
            logger.info("SUCCESSORSERVERWRITELOCK instruction received " + strResp); //WRITELOCK_SUCCESS writelockack <successor_addr>:<successor_ip>;<target_addr>:<target_ip>
            String[] arrResp = strResp.split("\\s+");
            String targetAddr = arrResp[2].split(";")[1];

            try {
                successorServer.connect();
                successorServer.sendMessage(new TextMessage("transferdatainvoke " + targetAddr));
                TextMessage resp = successorServer.receiveMessage();
                strResp = resp.getMsg().trim();
                
            } catch (Exception e) {
                logger.error("Error sending transferdatainvoke message in SUCCESSORSERVERWRITELOCK: [" + e + "]");  
                return;
            }

            logger.info("SUCCESSORSERVERWRITELOCK response received for transferdatainvoke: " + strResp); //TRANSFERDATAINVOKE_SUCCESS transferdata

            //send metadata update to all servers
            for (Map.Entry<String, IECSNode> entry : this.hashRing.nodeMap.entrySet()) {
                IECSNode curNode = entry.getValue();
                MetaDataUpdateSender sender = new MetaDataUpdateSender(curNode.getNodeHost(), curNode.getNodePort(), this.hashRing.getHashRingStr());
                Thread updateSender = new Thread(sender);
                updateSender.start();
            }

            //Send write lock release to successor server
            try {
                successorServer.connect();
                successorServer.sendMessage(new TextMessage("writelockrelease"));
                TextMessage resp = successorServer.receiveMessage();
                logger.info("writelockrelease received reponse: " + resp.getMsg().trim());

            } catch (Exception e) {
                logger.error("Error sending writelockrelease message in SUCCESSORSERVERWRITELOCK: [" + e + "]");  
                return;
            }

        } else if (inst == NextInst.SHUTDOWNNODE) {
            //strMsg should be <target_host>:<target_ip>
            String targetServerHost = strMsg.split(":")[0];
            int targetServerPort = Integer.parseInt(strMsg.split(":")[1]);

            IECSNode successorNode = this.hashRing.getSuccessor(this.hashRing.getHashVal(strMsg));

            //sent metadata update to successor
            CommClient successorServer = new CommClient(successorNode.getNodeHost(), successorNode.getNodePort());//todo: change host?
            try {
                successorServer.connect();
                successorServer.sendMessage(new TextMessage("metadataupdate " + this.hashRing.getHashRingStr()));
                TextMessage respMsg = successorServer.receiveMessage();
                logger.info("SHUTDOWNNODE metadataupdate received: " + respMsg.getMsg());
                successorServer.disconnect();
            } catch (Exception e) {
                logger.error("Error in sending metadataupdate to successor: " + e);
                return;
            }

            //send invoke transferalldata to target server (not needed?)
            CommClient targetServer = new CommClient(targetServerHost, targetServerPort);
            try {
                targetServer.connect();
                targetServer.sendMessage(new TextMessage("transferalldata " + successorNode.getNodeHost() + ":" + successorNode.getNodePort()));
                TextMessage respMsg = targetServer.receiveMessage();
                logger.info("SHUTDOWNNODE transferalldata received" + respMsg.getMsg());
                targetServer.disconnect();

            } catch (Exception e) {
                logger.error("Error in sending transferalldata to successor: " + e);
                return;
            }

            //after receiving transferalldata ACK, send metadata update to all clients
            for (Map.Entry<String, IECSNode> entry : this.hashRing.nodeMap.entrySet()) {
                IECSNode curNode = entry.getValue();
                MetaDataUpdateSender sender = new MetaDataUpdateSender(curNode.getNodeHost(), curNode.getNodePort(), this.hashRing.getHashRingStr());
                Thread updateSender = new Thread(sender);
                updateSender.start();
            }

            //TODO: initialize transfer of replica data to successor



        } else if (inst == NextInst.SHUTDOWNDETECTEDADDREPLICA) { //parameter: <shutdownnode_ip>:<shutdownnode_port>
            //coordinator is reassigned by the deleteNode function, replica needs to be copied (TODO)
            //strMsg


            // NodeDataUpdater firstPredUpdater = new NodeDataUpdater("addreplica", firstPredNode.getNodeHost(), firstPredNode.getNodePort(), curNodeAddr);
            // Thread firstPredThread = new Thread(firstPredUpdater);
            // firstPredThread.start();
        }
        
        else {
            logger.warn("Instruction not handled");
        }
    }

    public static void main(String[] args) {
        //       testing Hash Ring
        // HashRing r = new HashRing();

        // IECSNode a = new ECSNode("1", "localhost", 123);
        // r.insertNode("1", a);
        // System.out.println(r.getHashRingStr());
        // System.out.println(r.getPredecessor("1").getNodeName());


        // IECSNode b = new ECSNode("30", "localhost", 456);
        // r.insertNode("30", b);
        // System.out.println(r.getHashRingStr());

        // System.out.println(r.getSuccessor("30"));
        // System.out.println(r.getPredecessor("30"));

        // IECSNode n = r.getPredecessor("30");
        // n.setNodeHashRange("1000", "10001"); //is by reference

        // System.out.println("updated");
        // System.out.println(r.getHashRingStr());


        // System.out.println(n.getReplicaInfo()[0]==null); //is true

        


        // // //Insert c
        // IECSNode c = new ECSNode("10", "localhost", 789);
        // r.insertNode("10", c);

        // System.out.println(r.getSuccessor("10").getNodeName());
        // System.out.println(r.getPredecessor("10").getNodeName());


        // System.out.println(r.getSuccessor("30").getNodeName());
        // System.out.println(r.getPredecessor("30").getNodeName());

        // System.out.println(r.getSuccessor("1").getNodeName());
        // System.out.println(r.getPredecessor("1").getNodeName());

        // System.out.println(r.getHashRingStr());

        // r.deleteNode("10");
        // System.out.println(r.getHashRingStr());

        // IECSNode pred = r.getSuccessor("10");
        // System.out.println(pred.getNodeName());

        // System.exit(0);


        Options options = new Options();
        Option portOp = new Option("a", "address", true, "address");
        portOp.setRequired(false);
        options.addOption(portOp);
        Option addressOp = new Option("p", "port", true, "port number");
        addressOp.setRequired(false);
        options.addOption(addressOp);
        Option logPathOp = new Option("l", "logPath", true, "Log Path");
        logPathOp.setRequired(false);
        options.addOption(logPathOp);
        Option logLevelOp = new Option("ll", "logLevel", true, "Log Level");
        logLevelOp.setRequired(false);
        options.addOption(logLevelOp);
        // Option helpOp = new Option("h", "help", true, "Display Help");
        // helpOp.setOptionalArg(true);
        // helpOp.setRequired(false);
        // options.addOption(helpOp);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            logger.error("Cannot process cmd");
            System.exit(1);
        }

        //initialize ECS address
        String addr = "localhost";
        int port = 60000;
        if (cmd.hasOption("a")) {
            addr = cmd.getOptionValue("address");
        }
        if (cmd.hasOption("p")) {
            try {
                String portStr = cmd.getOptionValue("port");
                port = Integer.parseInt(portStr);
            } catch(NumberFormatException ex) {
                ex.printStackTrace();
            }
        }

        String logPath = null;
        if (!cmd.hasOption("l")) {
            Path currentRelativePath = Paths.get("logs/server.log");
            logPath = currentRelativePath.toString();
        } else{
            logPath = cmd.getOptionValue("logPath");
        }

        //Initialize Logger
        Level logLevel = Level.ALL;
        if (cmd.hasOption("ll")) {
            String input_logLevel = cmd.getOptionValue("logLevel");
            if(input_logLevel.equals(Level.DEBUG.toString())) {
                logLevel = Level.DEBUG;
            } else if(input_logLevel.equals(Level.INFO.toString())) {
                logLevel = Level.INFO;
            } else if(input_logLevel.equals(Level.WARN.toString())) {
                logLevel = Level.WARN;
            } else if(input_logLevel.equals(Level.ERROR.toString())) {
                logLevel = Level.ERROR;
            } else if(input_logLevel.equals(Level.FATAL.toString())) {
                logLevel = Level.FATAL;
            } else if(input_logLevel.equals(Level.OFF.toString())) {
                logLevel = Level.OFF;
            }
        }
 
        try{
            new LogSetup(logPath, logLevel);
            logger.info("Initiated ECSClient");
        } catch (Exception e) {
            logger.error("ECSClient failed to set up log: " + e);
        }

        //Create server configuration file
        File configFile = new File(CONFIGFILENAME);
        try {
            if (configFile.exists()) {
                configFile.delete();
            }
            configFile.createNewFile();
        } catch (IOException e) {
            logger.error("Failed to create server config file: " + e);
        }
      
        //Start failure detector thread
        ReentrantReadWriteLock configFileLock = new ReentrantReadWriteLock();
        // FailureDetector fd = new FailureDetector(addr, port, CONFIGFILENAME, 10000, configFileLock);
        // fd.startChecking();

        //Start ECS client
        ECSClient ecsClient = new ECSClient(addr, port, configFileLock);
        ecsClient.run();
        
    }
}
