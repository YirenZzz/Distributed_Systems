package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.Map;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import com.google.gson.Gson;

import app_kvClient.KVClient;
import logger.LogSetup;
import shared.messages.HandleStrResponse;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.TextMessage;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.KVMessage.StatusType;
import shared.HashRing;
import shared.communication.*;

import app_kvServer.cache.CacheInterface;
import app_kvServer.cache.FIFO;
import app_kvServer.cache.LFU;
import app_kvServer.cache.LRU;
import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import client.KVStore;
import ecs.IECSNode;

public class KVServer implements IKVServer, IServerObject { //IServerObject is used in the shared communication server class
	private static Logger logger = Logger.getRootLogger();
	private String host;
	private int port;
	private int cacheSize;
	private String strategy;

	private ServerSocket serverSocket;
    private boolean running;
	private FileDB fileDb;
	private CacheInterface cache = null;
	private ArrayList<Thread> connections;

	private static KVServer kvServer = null;
	
	private static String dataPath;

	public enum ServerState {
        ACTIVE,
        STOPPED,
        LOCKED
    };

	public ServerState status;
	// public Map<String, String[]> metadata;
	public String metadata;
	// public Map<String, String[]> metadata;
	public HashRing hashRing;

	private String serverName;
	private String targetServerName;
	private CommClient commClient;

	private int MAX_KEY_LEN = 20;
	private int MAX_VAL_LEN = 122880; //120kB

	private String ENTRYDELIMITER = ","; //todo: check if can use
	private String KEYVALDELIMITER = ":";
	
	private CountDownLatch shutDownLatch = new CountDownLatch(1);

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(String hostStr, int port, int cacheSize, String strategy, String name, String ECSHost, int ECSPort) {
		//convert host string to ip address
		String hostIp = "127.0.0.1";
		try {
            InetAddress hostAddr = InetAddress.getByName(hostStr);
            hostIp = hostAddr.getHostAddress();
        } catch (UnknownHostException ex) {
            logger.error("Unable to convert host " + hostStr + " to ip: " + ex);
        }
		
		this.host = hostIp;
		this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
		connections = new ArrayList<Thread>();
		running = initializeServer();

		logger.info("Connecting to ECS. Host: " + ECSHost + " Port: " + ECSPort);
		this.commClient = new CommClient(ECSHost, ECSPort);
		try{
			commClient.connect();
		} catch (Exception e) {
			logger.error ("Failed to connect to ECS." + e);
			System.exit(1);
		}
		logger.info ("Connection to ECS is created");
		
		//send message to notify ECS of adding new server
		try{
			commClient.sendMessage(new TextMessage("addnode " + hostIp + ":" + port));
			TextMessage sendMsgRes = commClient.receiveMessage(); //ECS should acknowledge the addnode message
			String strMsgRes = sendMsgRes.getMsg();
			logger.info("KVServer received: " + strMsgRes);

			//save metadata
			String[] arrMsgRes = strMsgRes.split("\\s+"); //ADDNODEACK_SUCCESS addnodeack metaData
			this.metadata = arrMsgRes[2];
			this.hashRing = new HashRing(arrMsgRes[2]);

		} catch (Exception e) {
			logger.warn("Failed to send addnode to ECS." + e);
		}
	}


	@Override
	public int getPort(){
		return this.serverSocket.getLocalPort();
	}

	@Override
    public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// return IKVServer.CacheStrategy.None; //unsure
		return CacheStrategy.valueOf(strategy);
	}

	@Override
    public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		String result = fileDb.get(key);
		if (result != null){
			return true;
		}
		else{
			return false;
		}
	}

	@Override
    public boolean inCache(String key){
		if (getCacheStrategy() == IKVServer.CacheStrategy.None) {
			return false;
		}
		if (cache.getCache(key)!=null) {
            return true;
        }
        return false;
	}

	@Override
    public String getKV(String key) throws Exception{ //todo: add not responsible here?
		try{
			String value = null;
			if (cache != null) {
				value = cache.getCache(key);
			}
			if (value != null) {
				return value;
			}

			String dbValue = fileDb.get(key);
			if (cache != null && dbValue != null) {
				cache.putCache(key, dbValue);
			}
			return dbValue;
			
		} catch (Exception e) {
			logger.error("Error! \n", e);
			throw e;
		}
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		//update cache
		if (CacheStrategy.valueOf(strategy) != IKVServer.CacheStrategy.None) {
			if (value == null) {
				cache.removeCache(key);
			} else {
				cache.putCache(key, value);
			}
		}
		
		//update fileDb
		boolean delRes = fileDb.del(key);
		if ((value == null || value == "" || value == "null")) { //type is delete
			if (!delRes){
				throw new IOException("del value failed"); //todo: exception in fileDB
			}
			return;
		}

		boolean putRes = fileDb.put(key, value);
		if (!putRes) {
			throw new IOException("put value failed"); //todo: exception in fileDB
		}
	}

	@Override
    public void clearCache(){
		if (CacheStrategy.valueOf(strategy) != IKVServer.CacheStrategy.None){
            cache.clearCache();
   		}
	}

	@Override
    public void clearStorage(){
		clearCache();
		fileDb.clear();//todo
	}

	private boolean isRunning() {
        return this.running;
    }
	
	@Override
    public void run(){
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                CommServer connection = 
	                		new CommServer(client, this);
	                
					if (isRunning()) { //added
						Thread client_thread = new Thread(connection);
						client_thread.start();
						connections.add(client_thread);
						logger.info("Connected to " 
								+ client.getInetAddress().getHostName() 
								+  " on port " + client.getPort());
					}
					
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
	}

	
	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());  
					
			fileDb = new FileDB(dataPath, getServerName());
			//todo: change according to strategy
			// cache = new FIFO(cacheSize);
			CacheStrategy cache_strategy = getCacheStrategy();
			if (cache_strategy == CacheStrategy.FIFO){
				cache = new FIFO(cacheSize);
			}
			else if(cache_strategy == CacheStrategy.LRU){
				cache = new LRU(cacheSize);
			}
			else if(cache_strategy == CacheStrategy.LFU){
				cache = new LFU(cacheSize);
			}

            return true;

        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

	
	@Override
    public void kill(){
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		running = false;
        try {
			for (int i = 0; i < connections.size(); i++) {
                connections.get(i).interrupt();
            }
			// for (Thread client : connections) {
            //     client.stop();
            // }
			if (serverSocket != null) {
				serverSocket.close();
			}
			
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	// @Override
	public void stop() {
		this.status = ServerState.STOPPED;
	}

	// @Override
	public void start() {
		this.status = ServerState.ACTIVE;
	}

	// @Override
	public void lockWrite() {
		this.status = ServerState.LOCKED;
	}

	// @Override
	public void unLockWrite() {
		this.status = ServerState.ACTIVE;
	}

	private String getMD5Hash(String data) throws NoSuchAlgorithmException {
        // Get an instance of MessageDigest with the MD5 algorithm
		MessageDigest md = MessageDigest.getInstance("MD5");
       	// Convert the input string to a byte array and compute the hash
	    byte[] hashBytes = md.digest(data.getBytes());
        
		// Convert the hash to a hex string representation
		StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }
	
	/*
	 * Get all data handled by a server
	 */
	public List<String> getTargetServerKeys(String md, String serverName) throws Exception {
		logger.info("getTargetServerKeys metadta: " + md +", serverName: " + serverName);
		String[] token = md.split(";");

		String start_pos = null;
		String end_pos= null;
		String serverHost = serverName.split(":")[0];
		int serverPort = Integer.parseInt(serverName.split(":")[1]);

		List<String> dbRes = new ArrayList<String>();
		if (this.fileDb == null) {
			logger.error("FILEDB is null");
			return null;
		}

		try {
			dbRes = this.fileDb.getAllKeys();
		} catch (Exception e) {
			logger.error("fileDB getAllKeys exception: "+ e);
			throw e;
		}


		// use hashRing to get data the targetServer is in charge of
		List<String> res = new ArrayList<String>();
		for(String r : dbRes) {
			//logger.info("strKey: " + r + " hash:"+ this.hashRing.getSuccessorInclusive(this.hashRing.getHashVal(r)));
			IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(this.hashRing.getHashVal(r));
			logger.info(responsibleNode.getNodePort()); //todo: get host?
			if (responsibleNode.getNodePort() == serverPort) {
				res.add(r);
			}
		}

		return res;
    	//String key_val = "";

		// Iterate through the source server's data and find all keys within the given range
		// try{
		// 	for (String key : fileDb.getAllKeys()) {
		// 		String hash_key = getMD5Hash(key);
		// 		logger.info("hash_key" + hash_key);

		// 		if (hash_key.compareTo(start_pos) >= 0 && hash_key.compareTo(end_pos) <= 0) {
		// 			key_val = key + ":" + fileDb.get(key) + ",";
		// 		}
		// 	}
		// } catch (Exception e) {
		// 	throw new Exception(e);
		// } 
		// return key_val;
	}

	public String getServerName(){
		String serverName = getHostname() + ":" + port;
		return serverName;
	}

	private String getHashedServerName() throws Exception{
		String serverName = getHostname() + ":" + port;
		return getMD5Hash(serverName);
	}
	
	public void updateMetadata(String metadata) {
		lockWrite();
		this.metadata = metadata;
		this.hashRing = new HashRing(this.metadata);
		unLockWrite();
	}

	public HandleStrResponse handleStrMsg(String strMsg) {
		String[] tokens = strMsg.trim().split("\\s+");
		
	    if (tokens[0].equals("get")) {
			logger.info("Processing get request: " + strMsg);
			String key = tokens[1]; //not in hex form

			if (this.status == KVServer.ServerState.STOPPED){
				logger.warn("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}
			// ? add isInHashedKeyRange method
			if (!this.isInHashedKeyRange(key)){
				logger.warn("SERVER_NOT_RESPONSIBLE");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (tokens.length > 2) {
				logger.error("GET_ERROR: invalid number of arguments");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			String value = null;		
			try {
				value = this.getKV(key);
			} catch (Exception e) {
				logger.error("GET_ERROR" + e);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (value != null){
				logger.info("value is not null");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GET_SUCCESS, key, value);
				return new HandleStrResponse(NextInst.NOINST, resp);
			} else { //todo: additioal error message for key not present
				logger.error("value is null");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

		} else if (tokens[0].equals("put")) {	
			String key = tokens[1];
			if (this.status == KVServer.ServerState.STOPPED){
				logger.error("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (this.status == KVServer.ServerState.LOCKED){
				logger.error("SERVER_WRITE_LOCK");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_WRITE_LOCK, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (!this.isInHashedKeyRange(key)){
				logger.error("SERVER_NOT_RESPONSIBLE");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Combine values string
			String value = null;
			if (tokens.length > 2) {
				value = tokens[2];
				int tokenIdx = 3;
				while (tokens.length > tokenIdx) {
					value = value + " " + tokens[tokenIdx];
					tokenIdx++;
				}

				//Check length of value
				if (key.getBytes().length > MAX_VAL_LEN) {
					logger.error("FAILED: value is too long");
					KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, key, "value is too long");
					return new HandleStrResponse(NextInst.NOINST, resp);
				}
			}

			KVMessage.StatusType sType = StatusType.PUT_SUCCESS;
			
			//Initialize return status type
			if (value == null || value == "" || value.equals("null")) {
				sType = StatusType.DELETE_SUCCESS;
				if (!this.inCache(key) && !this.inStorage(key)) {
					logger.info("put val is null, and key is not in storage");
					sType = StatusType.DELETE_ERROR;
					KVMessage resp = new KVMessageObject(sType, key, value);
					return new HandleStrResponse(NextInst.NOINST, resp);
				}
			} else if (this.inCache(key)) {
				sType = StatusType.PUT_UPDATE;
			} else if (this.inStorage(key)) {
				sType = StatusType.PUT_UPDATE;
			}

			try {
				this.putKV(key, value);
			} catch (Exception e) {
				logger.error("Exception in put: " + e);
				if (sType == StatusType.DELETE_SUCCESS) {
					sType = StatusType.DELETE_ERROR;
				} else {
					sType = StatusType.PUT_ERROR;
				}
			}

			KVMessage resp = new KVMessageObject(sType, key, value);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("keyrange")) {	//Keyrange request from client should reply the complete metadata
			if (this.status == KVServer.ServerState.STOPPED){
				logger.error("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, "server stopped", null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.KEYRANGE_SUCCESS, this.hashRing.getKeyRangeStr(),null); //? not sure if hashRing works
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("writelock")) {
			logger.info("server writelock received metadata " + tokens[1]);
			lockWrite();
			this.metadata = tokens[1];
			this.hashRing = new HashRing(this.metadata);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.WRITELOCK_SUCCESS, "writelockack", tokens[2]);//value contains target server address
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("transferdatainvoke")) {
			logger.info("Command transferdatainvoke, received" + strMsg.trim());
			if (tokens.length != 2) {
				logger.warn("FAILED: invalid number of arguments");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, "invalid number of arguments", null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Get keys belonging to the target server
			this.targetServerName = tokens[1]; 
			List<String> keys = new ArrayList<String>();
			try{
				keys = getTargetServerKeys(this.metadata, this.targetServerName);
			} catch (Exception e) {
				logger.error("Failed to getTargetServerKeys" + e);
			}

			//Send to the target Server
			String targetServerHost = this.targetServerName.split(":")[0];
			int targetServerPort = Integer.parseInt(this.targetServerName.split(":")[1]);
			KVStore cl = new KVStore(targetServerHost, targetServerPort);

			try  {
				cl.connect();
			} catch (Exception e) {
				logger.error("transferdatainvoke put connect exception" + e);
			}
			
			for (String k : keys) {
				try {
					cl.put(k, getKV(k)); //use cache
				} catch (Exception e) {
					logger.error("transferdatainvoke put data exception" + e);
				}
			}
			
			try  {
				cl.disconnect();
			} catch (Exception e) {
				logger.error("transferdatainvoke put disconnect exception" + e);
			}

			//delete data in current server //TODO: restore
			 for (String k : keys) {
			 	try {
			 		putKV(k, null);
			 	} catch (Exception e) {
			 		logger.error("Failed to delete key " + k + ": " + e);
			 	}
			 }

			//Reply to server
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.TRANSFERDATAINVOKE_SUCCESS, "transferdataack", null);
			return new HandleStrResponse(NextInst.NOINST, resp);
		} 
		
		else if (tokens[0].equals("metadataupdate")) {
			String ReceiveMetadata = tokens[1];
			updateMetadata(ReceiveMetadata);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.METADATAUPDATE_SUCCESS, "metadataupdateack", null);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("writelockrelease")) {
			unLockWrite();
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.WRITELOCKRELEASE_SUCCESS, "writelockreleaseack", null);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("transferalldata")) {
			logger.info("In_transferalldata, parameter " + tokens[1]);
			String successorServerName = tokens[1];
			//transfer data to successor server
			List<String> keys = new ArrayList<String>();
			if (fileDb != null) {
				keys = fileDb.getAllKeys();
			} else {
				logger.error("fileDB is null");
			}

			//Send to the target Server
			String successorServerHost = successorServerName.split(":")[0];
			int successorServerPort = Integer.parseInt(successorServerName.split(":")[1]);
			KVStore cl = new KVStore(successorServerHost, successorServerPort);

			try  {
				cl.connect();
			} catch (Exception e) {
				logger.error("transferalldata put connect exception" + e);
			}
			
			for (String k : keys) {
				try {
					cl.put(k, getKV(k)); //use cache
				} catch (Exception e) {
					logger.error("transferalldata put data exception" + e);
				}
			}
			
			try  {
				cl.disconnect();
			} catch (Exception e) {
				logger.error("transferalldata put disconnect exception" + e);
			}

			//delete data in current server
			for (String k : keys) {
				try {
					putKV(k, null);
				} catch (Exception e) {
					logger.error("transferalldata Failed to delete key " + k + ": " + e);
				}
			}

			this.shutDownLatch.countDown();
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.TRANSFERDATA_SUCCESS, "transferalldataack", null);
			return new HandleStrResponse(NextInst.FINISHTRANSFERALLDATA, resp);
		}
		
		else {
			logger.warn("Unknown command at server" + strMsg.trim()); //todo: null value causes exception in sendMessage
			KVMessage resp = new KVMessageObject(StatusType.FAILED, "unknown command", null);
			return new HandleStrResponse(NextInst.NOINST, resp);
		}
	}

	/**
     * Subsequent instructions
     * @return  cache size
     */
    public void handleNextInst(NextInst inst, String strMsg) {
		logger.info("in_handleNextInst, instruction is " + inst + ", parameter is " +strMsg);
		if (inst == NextInst.FINISHTRANSFERALLDATA){
			logger.info("FINISHEDTRANSFERALLDATAA");
			this.stopRunning();
			close();

			// //exit the current thread
			// Runtime.getRuntime().halt(0);
		}
		return;
	}

	public boolean isInHashedKeyRange(String key){
		if (this.hashRing == null) { //not using metadata
			return true; 
		}

		String hexKey = this.hashRing.getHashVal(key);
		IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(hexKey);

		if (responsibleNode.getNodePort() == this.port) { //todo: check host?
			return true;
		}
		logger.error("isInHashedKeyRange_getNodePort" + responsibleNode.getNodePort());//41313
		logger.error("server_port" + this.port); //correct

		return false;
 	}

	public void shutDown() {
		System.out.println("In Shutdown Method");
		logger.info("nodeMap_size" + this.hashRing.nodeMap.size());

		String strMsgRes = null;
		try {
			commClient.connect(); //connect to ECS
			commClient.sendMessage(new TextMessage("shutdownnode " + host + ":" + port));

			TextMessage sendMsgRes = commClient.receiveMessage(); //ECS should acknowledge the addnode message
			strMsgRes = sendMsgRes.getMsg();
			//commClient.disconnect();
			logger.info("KVServer shutdown received: " + strMsgRes);
			
			//server locks write after receiving the acknowledgement message
			this.lockWrite();

		} catch (Exception e) {
			logger.error("exception in sending shutdownnode to ecs" + e);
			return;
		}

		//Shutting down the last server
		if (strMsgRes.split("\\s+")[1].equals("shutdownnodedirectly")) {
			close();
			return;
		}


		logger.info("waiting for invoking transfer all data");

		//Wait for transfer data
		try {
			this.shutDownLatch.await();
		} catch (Exception e) {
			logger.error("Interrupt exception at await: " + e);
		}

		logger.info("finished invoking transfer all data");

		logger.info("Server Exiting");
	}
	
	public void stopRunning(){
		this.running = false;
	}


	public static void main(String[] args) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Shutdown Hook is running");
				//kvServer.stopRunning();
				if (kvServer != null) {
					kvServer.shutDown();
				}
			}
		});

    	try {
			Options options = new Options();
			Option portOp = new Option("p", "port", true, "port number");
			portOp.setRequired(false);
			options.addOption(portOp);
			Option addressOp = new Option("a", "address", true, "address");
			addressOp.setRequired(false);
			options.addOption(addressOp);
			Option dataPathOp = new Option("d", "dataPath", true, "Data path");
			dataPathOp.setRequired(false);
			options.addOption(dataPathOp);
			Option logPathOp = new Option("l", "logPath", true, "Log Path");
			logPathOp.setRequired(false);
			options.addOption(logPathOp);
			Option logLevelOp = new Option("ll", "logLevel", true, "Log Level");
			logLevelOp.setRequired(false);
			options.addOption(logLevelOp);
			
			Option strategyOp = new Option("s", "strategy", true, "Cache Strategy");
			strategyOp.setRequired(false);
			options.addOption(strategyOp);
			Option cacheSizeOp = new Option("c", "cacheSize", true, "Cache Size");
			cacheSizeOp.setRequired(false);
			options.addOption(cacheSizeOp);
			Option ECSAddrOp = new Option("b", "ECSAddr", true, "Log Level"); //host:port
			logLevelOp.setRequired(false);
			options.addOption(ECSAddrOp);

			Option helpOp = new Option("h", "help", true, "Display Help");
			helpOp.setOptionalArg(true);
			helpOp.setRequired(false);
			options.addOption(helpOp);

			HelpFormatter formatter = new HelpFormatter();
			CommandLineParser parser = new DefaultParser();
			CommandLine cmd;

			try {
				cmd = parser.parse(options, args);
			} 
			catch (ParseException e) {
				System.out.println(e.getMessage());
				formatter.printHelp("User Profile Info", options);
				System.exit(1);
				return;
			}
			if (args.length==1 && cmd.hasOption("h")) {
				formatter.printHelp("User Profile Info", options);
				System.exit(1);
				return;
			} 
			int port = Integer.parseInt(cmd.getOptionValue("port"));
			String address;
			String logPath;
			Level logLevel;
			
			if (!cmd.hasOption("a")) {
				//address = InetAddress.getLocalHost().toString();
				address = "localhost"; //default is localhost
			} else{
				address = cmd.getOptionValue("address");
			}

			if (!cmd.hasOption("d")) {
				Path currentRelativePath = Paths.get("");
				dataPath = currentRelativePath.toString();
			} else{
				dataPath = cmd.getOptionValue("dataPath");
			}

			if (!cmd.hasOption("l")) {
				Path currentRelativePath = Paths.get("logs/server.log");
				logPath = currentRelativePath.toString();
			} else{
				logPath = cmd.getOptionValue("logPath");
			}

			if (!cmd.hasOption("ll")) {
				logLevel = Level.ALL;
			} else{
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
				} else{
					logLevel = Level.ALL;
				}
			}

			String strategy;
			if (!cmd.hasOption("s")) {
				strategy = "FIFO";
			} else{
				strategy = cmd.getOptionValue("strategy");
			}

			int cacheSize = 4096;
			if (!cmd.hasOption("c")) {
				cacheSize = 4096;
			} else{
				try{
					cacheSize = Integer.parseInt(cmd.getOptionValue("cacheSize"));
				}catch(NumberFormatException ex){
					ex.printStackTrace();
				}
			}

			String ECSHost = "localhost";
			int ECSPort = 60000;
			if (cmd.hasOption("b")) { //todo: check if this argument is required
				try{
					String ECSAddr = cmd.getOptionValue("ECSAddr");
					String[] arrECSAddr = ECSAddr.split(":");
					ECSHost = arrECSAddr[0];
					ECSPort = Integer.parseInt(arrECSAddr[1]);
				}catch(NumberFormatException ex){
					ex.printStackTrace();
				}
			}

			new LogSetup(logPath, logLevel);
			kvServer = new KVServer(address, port, cacheSize, strategy, "local_server", ECSHost, ECSPort);
			kvServer.run();

		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
}
