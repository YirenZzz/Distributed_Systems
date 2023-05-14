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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;
import com.google.gson.Gson;

import app_kvClient.KVClient;
import logger.LogSetup;
import shared.messages.HandleStrResponse;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;
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
	public HashRing hashRing;

	private String serverName;
	private String targetServerName;
	private CommClient commClient;

	private int MAX_KEY_LEN = 20;
	private int MAX_VAL_LEN = 122880; //120kB
	
	private CountDownLatch shutDownLatch = new CountDownLatch(1);
	private String curNodeLowHash;
	private String curNodeHighHash;

	private Map<String, List<Integer>> listenerMap = new HashMap<>();

	/*
	 * Sends keys that the successor node is reponsible for to the success node
	 * Using put
	 */
	private class SuccDataUpdater implements Runnable {
		private IECSNode succNode; 
		private boolean toRemove;
		private FileDB fileDb;
		private String curNodeHost;
		private int curNodePort;

		/**
		 * @param rcvNodeAddr	receiver node ip
		 * @param rcvNodePort 	receiver node port
         * @param metaData      meta data
		 */
		public SuccDataUpdater(IECSNode s, boolean t, FileDB f, String h, int p) {
			this.succNode = s;
			this.toRemove = t; //whether to remove keys from the successor
			this.fileDb =  f;
			this.curNodeHost = h;
			this.curNodePort = p;
		}

		public void run() {
			logger.info("SuccDataUpdater thread");
			String succNodeName = succNode.getNodeHost() + ":" + succNode.getNodePort();

			// get all keys in the range of the new server node
			List<String> keys = new ArrayList<String>();
			try{
				if (toRemove == false) {
					keys = getTargetServerReadKeys(succNodeName);
				} else {
					//only remove keys that the current server is directly reponsible for
					String curNodeName = curNodeHost + ":" + curNodePort;
					keys = getTargetServerKeys(curNodeName);
				}

			} catch (Exception e) {
				logger.error("SuccDataUpdater getTargetServerReadKeys failed, toRemove is " + this.toRemove + ": " + e);
				return;
			}

			if (keys.size() == 0) {
				logger.info("SuccDataUpdater no data to send to successor, toRemove is " + this.toRemove);
				return;
			}

			// establish connection to the successor node
			CommClient succNodeClient = new CommClient(succNode.getNodeHost(), succNode.getNodePort());
			try  {
				succNodeClient.connect();
			} catch (Exception e) {
				logger.error("SuccDataUpdater commClient connect exception: " + e);
			}
			
			for (String k : keys) {
				try {
					String val = null;
					if (this.toRemove == false) {
						val = getKV(k);
						if (val == null || val == "null") {
							logger.error("SuccDataUpdater_ALREADY_NULL");
						}
					}
					String succMsg = "putreplica " + k + " " + val;
					succNodeClient.sendMessage(new TextMessage(succMsg));

				} catch (Exception e) {
					logger.error("SuccDataUpdater putreplica exception for key " + k + ": " + e);
				}
			}
			
			try  {
				succNodeClient.disconnect();
			} catch (Exception e) {
				logger.error("SuccDataUpdater commClient disconnect exception: " + e);
			}

		}
	}

	/*
	 * Sends keys that the successor node is reponsible for to the success node
	 * Using put
	 */
	private class SubscriberSender implements Runnable {
		private IECSNode succNode; 
		private Map<String, List<Integer>> listenerMap;
		private HashRing hashRing;

		public SubscriberSender(IECSNode s, Map<String, List<Integer>> l, HashRing h) {
			this.succNode = s;
			this.listenerMap =l;
			this.hashRing = h;
		}

		public void run() {
			logger.info("SubscriberSender thread");
			String succNodeName = succNode.getNodeHost() + ":" + succNode.getNodePort();

			Iterator iter = listenerMap.entrySet().iterator();
			String subscriberInfo = "subscribers "; //subscribers <key1>,<port1>,<port2>;<key2>,<port1>,<port2>;
			List<String> listKeys = new ArrayList<String>();

			while (iter.hasNext()) {
				Map.Entry mapElement = (Map.Entry)iter.next();
				String curKey = (String)mapElement.getKey();
				IECSNode curKeyRespNode = hashRing.getSuccessorInclusive(hashRing.getHashVal(curKey));
				if (curKeyRespNode.getNodePort() != succNode.getNodePort()) {
					continue;
				}

				listKeys.add(curKey);
				subscriberInfo = subscriberInfo + curKey;
				List<Integer> curInfoList = (List<Integer>)mapElement.getValue();
				for (int i = 0; i < curInfoList.size(); i++) {
					int curPort = curInfoList.get(i);
					subscriberInfo = subscriberInfo + "," + Integer.toString(curPort);
				}
				subscriberInfo = subscriberInfo + ";";
			}

			if (listKeys.size() > 0) {
				try{
					CommClient succClient = new CommClient(succNode.getNodeHost(), succNode.getNodePort());
					succClient.connect();
					succClient.sendMessage(new TextMessage(subscriberInfo));
					TextMessage succResp  = succClient.receiveMessage();
					logger.info("SubscriberSender received message: " + succResp.getMsg());
					succClient.disconnect();
				} catch (Exception e) {
					logger.error("SubscriberSender error sending subscriber information: " + e);
				}

				//delete the sent values
				for (String str : listKeys) {
					listenerMap.remove(str);
				}
			}
		}
	}


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

		this.fileDb = new FileDB(dataPath, host+":"+port);
		if (strategy == "FIFO"){
			this.cache = new FIFO(cacheSize);
		}
		else if(strategy == "LRU"){
			this.cache = new LRU(cacheSize);
		}
		else if(strategy == "LFU"){
			this.cache = new LFU(cacheSize);
		}

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
			String[] arrMsgRes = strMsgRes.split("\\s+"); //receives: ADDNODEACK_SUCCESS addnodeack metaData
			this.metadata = arrMsgRes[2];
			this.hashRing = new HashRing(arrMsgRes[2]);

		} catch (Exception e) {
			logger.error("Failed to send addnode to ECS." + e);
			return;	
		}

		String curNodeAddr = this.host + ":" + this.port;
		String curNodeKey = this.hashRing.getHashVal(curNodeAddr);
		IECSNode curNode = hashRing.nodeMap.get(curNodeKey);
		if (curNode != null) {
			this.curNodeLowHash = curNode.getNodeHashRange()[0]; //todo: remove this value
			this.curNodeHighHash = curNode.getNodeHashRange()[1];
		} else {
			logger.error("KVServer curNode is null");
			return;
		}

	}


	@Override
	public int getPort(){
		return this.serverSocket.getLocalPort();
	}

	@Override
	public String getHostname() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error("Failed to get hostname", e);
        }
        return hostname;
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
    public String getKV(String key) throws Exception{
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
				throw new IOException("del value failed");
			}
			return;
		}

		boolean putRes = fileDb.put(key, value);
		if (!putRes) {
			throw new IOException("put value failed");
		}
	}

    public boolean subKV(String key, int port) {
		//Do not subscribe if key is not in storage
		if (!this.inCache(key) && !this.inStorage(key)) {
			logger.info("Key " + key + " is not in storage");
			return false;
		}

		//save listener to map
		if (listenerMap.containsKey(key)) {
			List<Integer> curKeyListeners = listenerMap.get(key);
			curKeyListeners.add(port);
		} else {
			ArrayList<Integer> curKeyListeners = new ArrayList<>(Arrays.asList(port));
			listenerMap.put(key, curKeyListeners);
		}

		return true;
	}

	public boolean unsubKV(String key, int port) {
		//Do not unsubscribe if key is not in storage
		if (!this.inCache(key) && !this.inStorage(key)) {
			logger.info("unsubKV key " + key + " is not in storage");
			return false;
		}

		//Do not unsubscribe if key is not subscribed
		if (!listenerMap.containsKey(key)) {
			logger.info("unsubKV key " + key + " is not yet being subscribed");
			return false;
		}

		//save listener to map //TODO
		List<Integer> curKeyListeners = listenerMap.get(key);
		if (!curKeyListeners.contains(port)) {
			logger.info("unsubKV port " + port + " did not subscribe to key " + key);
			return false;
		}

		curKeyListeners.remove(Integer.valueOf(port)); //remove by value;
		return true;
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
	public List<String> getTargetServerKeys(String serverName) throws Exception {
		logger.info("getTargetServerKeys serverName: " + serverName);

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
	}


	/*
	 * Get all data handled by server's read
	 */
	public List<String> getTargetServerReadKeys(String serverName) throws Exception {
		logger.info("getTargetServerReadKeys serverName: " + serverName);

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

		logger.info("TESTING_getTargetServerReadKeys");
		logger.info("Serverport is " + serverPort);

		// for (String k : dbRes) {
		// 	logger.info("k");
		// }


		// use hashRing to get data the targetServer is in charge of
		List<String> res = new ArrayList<String>();
		for(String r : dbRes) {
			String curVal = getKV(r);
			if (curVal == "null" || curVal == null) {
				logger.warn("getTargetServerReadKeys value is null for key" + r);
				continue;
			}

			//logger.info("strKey: " + r + " hash:"+ this.hashRing.getSuccessorInclusive(this.hashRing.getHashVal(r)));
			IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(this.hashRing.getHashVal(r));
			if (responsibleNode.getNodePort() == serverPort) {
				res.add(r);
				continue;
			}

			String responsibleNodeName = responsibleNode.getNodeHost() + ":" + responsibleNode.getNodePort();
			IECSNode replica1 = this.hashRing.getSuccessor(this.hashRing.getHashVal(responsibleNodeName));

			if (replica1.getNodePort() == serverPort) {
				res.add(r);
				continue;
			}

			String replica1NodeName = replica1.getNodeHost() + ":" + replica1.getNodePort();
			IECSNode replica2 = this.hashRing.getSuccessor(this.hashRing.getHashVal(replica1NodeName));

			if (replica2.getNodePort() == serverPort) {
				res.add(r);
				continue;
			}
		}

		return res;
	}




	public String getServerName(){
		// String serverName = getHostname() + ":" + port;
		String serverName = getHostname() + ":" + getPort();
		// try {
		// 	serverName = getHostname() + ":" + getPort();
		// } catch (UnknownHostException e) {
        //     logger.error("Failed to get hostname", e);
        // }
		return serverName;
	}

	private String getHashedServerName() throws Exception{
		// String serverName = getHostname() + ":" + port;
		String serverName = getServerName();
		// try {
		// 	serverName = getServerName();
		// } catch (UnknownHostException e) {
        //     logger.error("Failed to get hostname", e);
        // }
		return getMD5Hash(serverName);
	}
	
	public void updateMetadata(String metadata) {
		//lockWrite();
		this.metadata = metadata;
		this.hashRing = new HashRing(this.metadata);
		//unLockWrite();
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

			String hex_key = this.hashRing.getHashVal(key);
			IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(hex_key);
			
			String responsible_servername = responsibleNode.getNodeHost()  + ":" + responsibleNode.getNodePort();
			String hex_responsible_servername = this.hashRing.getHashVal(responsible_servername);

			IECSNode rep1 = this.hashRing.getSuccessor(hex_responsible_servername);
			String rep1Host = rep1.getNodeHost();
			int rep1Port = rep1.getNodePort();

			String hex_rep1_servername = this.hashRing.getHashVal(rep1Host+":"+rep1Port);
			
			IECSNode rep2 = this.hashRing.getSuccessor(hex_rep1_servername);
			String rep2Host = rep2.getNodeHost();
			int rep2Port = rep2.getNodePort();

			//check if the current server is responsible 
			if (!(responsibleNode.getNodePort() == this.port || rep1Port==this.port || rep2Port == this.port)) {
				logger.warn("SERVER_NOT_RESPONSIBLE for " + key);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			// if (!this.isInHashedKeyRange(key)){
			// 	logger.warn("SERVER_NOT_RESPONSIBLE");
			// 	KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
			// 	return new HandleStrResponse(NextInst.NOINST, resp);		
			// }

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

		}

		else if (tokens[0].equals("put")) {	
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
					logger.error("Failed: value is too long");
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

			if (sType == StatusType.PUT_SUCCESS || sType == StatusType.DELETE_SUCCESS || sType == StatusType.PUT_UPDATE) {	
				String hex_key = this.hashRing.getHashVal(key);
				IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(hex_key);

				// this server is the coordinator
				String current_servername = responsibleNode.getNodeHost()  + ":" + responsibleNode.getNodePort();
				String hex_current_servername = this.hashRing.getHashVal(current_servername);

				IECSNode rep1 = this.hashRing.getSuccessor(hex_current_servername);
				String rep1Host = rep1.getNodeHost();
				int rep1Port = rep1.getNodePort();

				String hex_rep1_servername = this.hashRing.getHashVal(rep1Host+":"+rep1Port);
				
				IECSNode rep2 = this.hashRing.getSuccessor(hex_rep1_servername);
				String rep2Host = rep2.getNodeHost();
				int rep2Port = rep2.getNodePort();

				// <rep1_ip>:<rep1_port>,<rep2_ip>:<rep2_port>,<curNode_ip>:<curNode_port>;key value
				String strParam = rep1Host + ":" + rep1Port + "," + rep2Host + ":" + rep2Port + "," + current_servername + ";" + key + " " + value; //key might contain ','
				return new HandleStrResponse(NextInst.UPDATEREPLICA, resp, strParam);
			}

			return new HandleStrResponse(NextInst.NOINST, resp);
		}  

		else if (tokens[0].equals("beginTX")) {	

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

			// create unique transactionId use current timestamp
			long transactionId = System.currentTimeMillis();
			String transactionIdStr = Long.toString(transactionId);

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.BEGINTX_SUCCESS, "beginTXack", transactionIdStr);
			return new HandleStrResponse(NextInst.NOINST, resp);
		}  

		else if (tokens[0].equals("commitTX")) {	
			String transactionId = tokens[1];
			String[] transactionLogOperation = tokens[2].split(";");
			
			// KVStore cl = new KVStore(getHostname(), getPort());
			KVStore cl = new KVStore(getHostname(), port);

			try  {
				cl.connect();
			} catch (Exception e) {
				logger.error("commitTX connect exception" + e);
			}
			
			for (int i = 0; i < transactionLogOperation.length; i++) {
				String[] operation = transactionLogOperation[i].split(",");
				if (operation.length < 2) {
					logger.error("transaction not enough arguments"); //TODO: rollback
					KVMessage resp = new KVMessageObject(KVMessage.StatusType.TRANSACTION_ERROR, transactionId, null);
					return new HandleStrResponse(NextInst.NOINST, resp);
				
				} else if (operation.length == 3) {
					if (operation[0].equals("put")){
						String key = operation[1];	
						String value = operation[2];	
						try {
							KVMessage res = cl.put(key, value); //roll back in client side
							if (res.getStatus() != KVMessage.StatusType.PUT_SUCCESS && res.getStatus() != KVMessage.StatusType.PUT_UPDATE &&
									res.getStatus() != KVMessage.StatusType.DELETE_SUCCESS) {
								logger.warn("Error executing put operation on key " + key);
								KVMessage resp = new KVMessageObject(KVMessage.StatusType.ROLLED_BACK, transactionId, key);
								return new HandleStrResponse(NextInst.NOINST, resp);
							}

						} catch (Exception e) {
							logger.error("commitTX put data exception" + e);
							KVMessage resp = new KVMessageObject(KVMessage.StatusType.ROLLED_BACK, transactionId, null);
							return new HandleStrResponse(NextInst.NOINST, resp);
						}
					}
				}

			}
			
			try  {
				cl.disconnect();
			} catch (Exception e) {
				logger.error("commitTX disconnect exception" + e);
			}

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.COMMITTX_SUCCESS, "committxack", transactionId);
			return new HandleStrResponse(NextInst.NOINST, resp);
			
		}  
		
		else if (tokens[0].equals("sub")) {
			logger.info("Processing subscribe request: " + strMsg);
			int port = 0;
			String key = null;
			try {
				port = Integer.parseInt(tokens[1]);
				key = tokens[2]; //not in hex form
			} catch (Exception e) {
				logger.error("Error parsing param for: " + strMsg);
			}

			if (this.status == KVServer.ServerState.STOPPED) {
				logger.warn("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			String hex_key = this.hashRing.getHashVal(key);
			IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(hex_key);

			//check if the current server is responsible 
			if (responsibleNode.getNodePort() != this.port ) {
				logger.warn("Subscribe command SERVER_NOT_RESPONSIBLE for " + key);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (tokens.length > 3) {
				logger.error("GET_ERROR: invalid number of arguments");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SUB_ERROR, key, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			boolean res = this.subKV(key, port);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.SUB_SUCCESS, key, null);
			if (!res){
				resp = new KVMessageObject(KVMessage.StatusType.SUB_ERROR, key, null);
			}
			return new HandleStrResponse(NextInst.NOINST, resp);
		}

		else if (tokens[0].equals("unsub")) { //TODO
			logger.info("Processing unsubscribe request: " + strMsg); //unsub port key
			int port = 0;
			String key = null;

			try {
				port = Integer.parseInt(tokens[1]);
				key = tokens[2];
			} catch (Exception e) {
				logger.error("Error parsing param for: " + strMsg);
			}

			if (this.status == KVServer.ServerState.STOPPED) {
				logger.warn("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			String hex_key = this.hashRing.getHashVal(key);
			IECSNode responsibleNode = this.hashRing.getSuccessorInclusive(hex_key);
			
			//check if the current server is responsible 
			if (responsibleNode.getNodePort() != this.port ) {
				logger.warn("Subscribe command SERVER_NOT_RESPONSIBLE for " + key);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (tokens.length > 3) {
				logger.error("UNSUB_ERROR: invalid number of arguments");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.UNSUB_ERROR, key, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			boolean res = this.unsubKV(key, port);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.UNSUB_SUCCESS, key, null);
			if (!res){
				resp = new KVMessageObject(KVMessage.StatusType.UNSUB_ERROR, key, null);
			}
			return new HandleStrResponse(NextInst.NOINST, resp);
		}
		
		else if (tokens[0].equals("subscribers")) { //save subscriber information
			if (tokens.length < 2) {
				logger.error("subscribers not enough arguments");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SUB_ERROR, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			// "<key>,port1,port2;<key>,port1;"
			String[] kvPairs = tokens[1].split(";");
			for (int i = 0; i < kvPairs.length; i++) {
				String[] vals = kvPairs[i].split(",");
				if (vals.length < 2) {
					logger.warn("Key " + vals[0] + "not enough arguments");
					continue;
				}
				if (listenerMap.containsKey(vals[0])){
					List<Integer> curKeyListeners = listenerMap.get(vals[0]);
					for (int j = 1; j < vals.length; j++) {
						curKeyListeners.add(Integer.parseInt(vals[j]));
					}
				} else {
					List<Integer> curList = new ArrayList<>();
					for (int j = 1; j < vals.length; j++) {
						curList.add(Integer.parseInt(vals[j]));
					}

					listenerMap.put(vals[0], curList);
				}
			}

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.SUB_SUCCESS, null, null);
			return new HandleStrResponse(NextInst.NOINST, resp);
		}

		else if (tokens[0].equals("getAllKeys")) { //get all keys, sent from client
			//return all keys from the current server, and query the other servers connected to ECS
			//format is <key1>,<key2>,<key3>
			List<String> keys = new ArrayList<String>();
			try{
				keys = getTargetServerKeys(this.host + ":" + this.port);
			} catch (Exception e) {
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GETALLKEYS_ERROR, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}
			
			//query other servers
			String queryMsg = "getAllKeysSubQuery";
			for (Map.Entry<String, IECSNode> nodeMapEntry : hashRing.nodeMap.entrySet()) {
				IECSNode curNode = nodeMapEntry.getValue();
				try{
					CommClient curClient = new CommClient(curNode.getNodeHost(), curNode.getNodePort());
					curClient.connect();
					curClient.sendMessage(new TextMessage(queryMsg));
					TextMessage clientResp  = curClient.receiveMessage();
					String strRespMsg = clientResp.getMsg(); //TODO
					logger.info("getAllKeys received message: " + strRespMsg);
					curClient.disconnect();

					//retrieve the list of keys in the current server
					String[] arrRespMsg = strRespMsg.split("\\s+"); 
					if (arrRespMsg.length < 2) {
						continue;
					}

					String[] arrListKeys = arrRespMsg[1].split(",");
					List<String> listKeys = Arrays.asList(arrListKeys);
					listKeys.removeAll(keys); //remove duplicates
					keys.addAll(listKeys);

				} catch (Exception e) {
					logger.error("Error querying keys for server " + curNode.getNodePort() + ", " + e);
				}
			}

			String strKeysList = String.join(",", keys);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.GETALLKEYS_SUCCESS, strKeysList, null);
			return new HandleStrResponse(NextInst.NOINST, resp);
		}

		else if (tokens[0].equals("getAllKeysSubQuery")) {  //get all keys, queried from a server
			List<String> keys = new ArrayList<String>();
			try {
				keys = getTargetServerKeys(this.host + ":" + this.port);
			} catch (Exception e) {
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.GETALLKEYS_ERROR, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			String strKeysList = String.join(",", keys);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.GETALLKEYS_SUCCESS, strKeysList, null);
			return new HandleStrResponse(NextInst.NOINST, resp);
		}

		else if (tokens[0].equals("putreplica")) { //insert directly without checking whether is responsible, or sending to replica
			String key = tokens[1];
			if (this.status == KVServer.ServerState.STOPPED){
				logger.error("putreplica SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			if (this.status == KVServer.ServerState.LOCKED){
				logger.error("putreplica SERVER_WRITE_LOCK");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_WRITE_LOCK, null, null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("putreplica failed: key is too long " + tokens[1]);
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
					logger.error("putreplica failed: value is too long");
					KVMessage resp = new KVMessageObject(KVMessage.StatusType.FAILED, key, "value is too long");
					return new HandleStrResponse(NextInst.NOINST, resp);
				}
			}

			KVMessage.StatusType sType = StatusType.PUT_SUCCESS;
			
			//Initialize return status type
			if (value == null || value == "" || value.equals("null")) {
				sType = StatusType.DELETE_SUCCESS;
				if (!this.inCache(key) && !this.inStorage(key)) {
					logger.info("putreplica val is null, and key is not in storage");
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

			KVMessage resp = new KVMessageObject(KVMessage.StatusType.KEYRANGE_SUCCESS, this.hashRing.getKeyRangeStr(),null);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("keyrange_read")) {
			if (this.status == KVServer.ServerState.STOPPED){
				logger.error("SERVER_STOPPED");
				KVMessage resp = new KVMessageObject(KVMessage.StatusType.SERVER_STOPPED, "server stopped", null);
				return new HandleStrResponse(NextInst.NOINST, resp);
			}
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.KEYRANGE_READ_SUCCESS, this.hashRing.getHashRingReadStr(),null);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("writelock")) {
			logger.info("server writelock received metadata " + tokens[1]);
			lockWrite();

			//find the current node in the hash ring
			IECSNode curNode = this.hashRing.getCurNode(this.host, this.port);
			String curNodeName = this.host + ":" + this.port;
			boolean nodeFound = true;
			IECSNode succ1Node = null, succ2Node = null;

			if (curNode == null) {
				logger.error("writelock curNode does not exist");
				nodeFound = false;
			} else {
				//record the successor nodes
				succ1Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(curNodeName));
				String succ1NodeName = succ1Node.getNodeHost() + ":" + succ1Node.getNodePort();
				succ2Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ1NodeName));
			}

			this.metadata = tokens[1];
			this.hashRing = new HashRing(this.metadata);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.WRITELOCK_SUCCESS, "writelockack", tokens[2]);//value contains target server address

			// find successor nodes in the new metadata
			IECSNode curNodeAfter =  this.hashRing.getCurNode(this.host, this.port);
			IECSNode succ1NodeAfter = null, succ2NodeAfter = null;
			if (curNodeAfter == null) {
				logger.error("metadataupdate curNodeAfter does not exists");
				nodeFound = false;
			} else {
				//record the successor nodes
				succ1NodeAfter = this.hashRing.getSuccessor(this.hashRing.getHashVal(curNodeName));
				String succ1NodeAfterName = succ1NodeAfter.getNodeHost() + ":" + succ1NodeAfter.getNodePort();
				succ2NodeAfter = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ1NodeAfterName));
			}
			
			//does not need to update successor nodes if the successor nodes are not changed
			if (this.hashRing.nodeMap.size() <= 1 || !nodeFound || (succ1Node.getNodePort() == succ1NodeAfter.getNodePort() && succ2Node.getNodePort() == succ2NodeAfter.getNodePort())) {
				logger.info("writelock_returning_noinst");
				return new HandleStrResponse(NextInst.NOINST, resp);
			}
			return new HandleStrResponse(NextInst.AFTERMETADATAUPDATE, resp);
			// this.metadata = tokens[1];
			// this.hashRing = new HashRing(this.metadata);
			// KVMessage resp = new KVMessageObject(KVMessage.StatusType.WRITELOCK_SUCCESS, "writelockack", tokens[2]);//value contains target server address
			// return new HandleStrResponse(NextInst.NOINST, resp);

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
				keys = getTargetServerKeys(this.targetServerName);
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

			//delete data in current server
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
		} else if (tokens[0].equals("metadataupdate")) { //record the previous successors, if there is a change in successor port, initialize data transfer
			logger.info("IN_metadataupdate" + this.hashRing.getHashRingStr());
			//find the current node in the hash ring
			IECSNode curNode = this.hashRing.getCurNode(this.host, this.port);
			String curNodeName = this.host + ":" + this.port;
			boolean nodeFound = true;
			IECSNode succ1Node = null, succ2Node = null;

			if (curNode == null) {
				logger.error("metadataupdate curNode does not exist");
				nodeFound = false;
			} else {
				//record the successor nodes
				succ1Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(curNodeName));
				String succ1NodeName = succ1Node.getNodeHost() + ":" + succ1Node.getNodePort();
				succ2Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ1NodeName));
			}

			String ReceiveMetadata = tokens[1];
			updateMetadata(ReceiveMetadata);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.METADATAUPDATE_SUCCESS, "metadataupdateack", null);

			// find successor nodes in the new metadata
			IECSNode curNodeAfter =  this.hashRing.getCurNode(this.host, this.port);
			IECSNode succ1NodeAfter = null, succ2NodeAfter = null;
			if (curNodeAfter == null) {
				logger.error("metadataupdate curNodeAfter does not exists");
				nodeFound = false;
			} else {
				//record the successor nodes
				succ1NodeAfter = this.hashRing.getSuccessor(this.hashRing.getHashVal(curNodeName));
				String succ1NodeAfterName = succ1NodeAfter.getNodeHost() + ":" + succ1NodeAfter.getNodePort();
				succ2NodeAfter = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ1NodeAfterName));
			}
			
			//does not need to update successor nodes if the successor nodes are not changed
			if (this.hashRing.nodeMap.size() <= 1 || !nodeFound || (succ1Node.getNodePort() == succ1NodeAfter.getNodePort() && succ2Node.getNodePort() == succ2NodeAfter.getNodePort())) {
				logger.info("metadataupdate_returning_noinst");
				return new HandleStrResponse(NextInst.NOINST, resp);
			}
			return new HandleStrResponse(NextInst.AFTERMETADATAUPDATE, resp);
			
		} else if (tokens[0].equals("writelockrelease")) {
			unLockWrite();
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.WRITELOCKRELEASE_SUCCESS, "writelockreleaseack", null);
			return new HandleStrResponse(NextInst.NOINST, resp);

		} else if (tokens[0].equals("transferalldata")) { //used in node shutdown
			logger.info("In_transferalldata, parameter " + tokens[1]);
			String successorServerName = tokens[1];
			//transfer data to successor server
			List<String> keys = new ArrayList<String>();
			if (fileDb != null) {
				keys = fileDb.getAllKeys();
			} else {
				logger.error("fileDB is null");
			}

			String successorServerHost = successorServerName.split(":")[0];
			int successorServerPort = Integer.parseInt(successorServerName.split(":")[1]);
			
			// Send subscriber to the target server
			// "subscribers <key>,port1,port2;<key>,port1;"
			if (listenerMap.size() > 0) {
				String subscriberInfo = "subscribers ";
				Iterator iter = listenerMap.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry mapElement = (Map.Entry)iter.next();
					subscriberInfo = subscriberInfo + mapElement.getKey();
					List<Integer> curInfoList = (List<Integer>)mapElement.getValue();
					for (int i = 0; i < curInfoList.size(); i++) {
						int curPort = curInfoList.get(i);
						subscriberInfo = subscriberInfo + "," + Integer.toString(curPort);
					}
					subscriberInfo = subscriberInfo + ";";
				}

				try{
					CommClient succClient = new CommClient(successorServerHost, successorServerPort);
					succClient.connect();
					succClient.sendMessage(new TextMessage(subscriberInfo));
					TextMessage succResp  = succClient.receiveMessage();
					logger.info("subKV received message: " + succResp.getMsg());
					succClient.disconnect();
				} catch (Exception e) {
					logger.error("Error sending subscriber information: " + e);
				}
			}

			//Send k-v pairs to the target Server
			KVStore cl = new KVStore(successorServerHost, successorServerPort);
			try  {
				cl.connect();
			} catch (Exception e) {
				logger.error("transferalldata put connect exception" + e);
			}

			//Transfer k-v pairs
			for (String k : keys) { //only transfer the data that the current server is directly responsible for
				String keyHashVal = this.hashRing.getHashVal(k);
				IECSNode keyRespNode = this.hashRing.getSuccessorInclusive(keyHashVal);

				if (keyRespNode != null && keyRespNode.getNodePort() == this.port) { //also check host
					try {
						cl.put(k, getKV(k)); //uses cache
					} catch (Exception e) {
						logger.error("transferalldata put data exception" + e);
					}
				}
			}
			
			try  {
				cl.disconnect();
			} catch (Exception e) {
				logger.error("transferalldata put disconnect exception" + e);
			}

			//delete data in current server
			for (String k : keys) { //deleting all data, including replica
				try {
					putKV(k, null);
				} catch (Exception e) {
					logger.error("transferalldata Failed to delete key " + k + ": " + e);
				}
			}

			this.shutDownLatch.countDown();
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.TRANSFERDATA_SUCCESS, "transferalldataack", null);
			return new HandleStrResponse(NextInst.FINISHTRANSFERALLDATA, resp);
		
		} else {
			logger.warn("Unknown command at server: " + strMsg.trim());
			KVMessage resp = new KVMessageObject(StatusType.FAILED, "Unknown Command", null);
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

			// exit the current thread
			// Runtime.getRuntime().halt(0);
		} else if (inst == NextInst.UPDATEREPLICA) {
			logger.info("UPDATEREPLICA"); 

			//<rep1_ip>:<rep1_port>,<rep2_ip>:<rep2_port>,<curNode_ip>:<curNode_port>;key value
			String[] arrParam = strMsg.split(";");
			String[] arrRep1Info = arrParam[0].split(",")[0].split(":");
			int rep1Port = Integer.parseInt(arrRep1Info[1]);
			
			String[] arrRep2Info = arrParam[0].split(",")[1].split(":");
			int rep2Port = Integer.parseInt(arrRep2Info[1]);

			String[] curNodeInfo = arrParam[0].split(",")[2].split(":");
			int curNodePort = Integer.parseInt(curNodeInfo[1]);

			String[] arrKV = arrParam[1].split("\\s+");
			String key = arrKV[0];
			String value = "null";

			if (arrKV.length > 1) {
				value = arrKV[1];
				for (int i = 2; i < arrKV.length; i++) {
					value = value + " " + arrKV[i]; //would be wrong if value contains multiple adjacent spaces
				}
			}

			//notify subscribers
			logger.info("notifying subscribers");
			if (listenerMap.containsKey(key)) {
				List<Integer> curKeyListeners = listenerMap.get(key);

				for (int i = 0; i < curKeyListeners.size(); i++) {
					int curPort = curKeyListeners.get(i);
					CommClient cl = new CommClient("127.0.0.1", curPort);	//assuming clients are on the same maching
					logger.info("notifying subscriber " + Integer.toString(curPort));

					try {
						cl.connect(); 
						String strNotice = "The value of key \"" + key + "\" has been changed to \"" + value + "\"";
						if (value.equals("null")) {
							strNotice = "Key \"" + key + "\" has been deleted";
						}

						cl.sendMessage(new TextMessage(strNotice));
						TextMessage respMsg  = cl.receiveMessage();
						logger.info("subKV received message: " + respMsg.getMsg());
						cl.disconnect();

					} catch (Exception e) {
						logger.error ("subKV failed to send message to client " + port + ": " + e);
					}
				}
			}

			//send to the first replica
			if (rep1Port != curNodePort) { // use thread
				logger.info("UPDATEREPLICA sending to replica 1");
				CommClient rep1Client = new CommClient(arrRep1Info[0], rep1Port);

				try {
					rep1Client.connect();
					rep1Client.sendMessage(new TextMessage("putreplica " + key + " " + value));
					TextMessage rep1Res = rep1Client.receiveMessage();
					logger.info("UPDATEREPLICA received from replica 1: " + rep1Res.getMsg());
					rep1Client.disconnect();
				} catch (Exception e) {
					logger.warn("UPDATEREPLICA error updating replica 1 " + rep1Port + ", " + e);
				}
			}

			//send to the second replica
			if (rep2Port != rep1Port && rep2Port != curNodePort) {
				logger.info("UPDATEREPLICA sending to replica 2");
				CommClient rep2Client = new CommClient(arrRep2Info[0], rep2Port);

				try {
					rep2Client.connect();
					rep2Client.sendMessage(new TextMessage("putreplica " + key + " " + value));
					TextMessage rep2Res = rep2Client.receiveMessage();
					logger.info("UPDATEREPLICA received from replica 2: " + rep2Res.getMsg());
					rep2Client.disconnect();
				} catch (Exception e) {
					logger.warn("UPDATEREPLICA error updating replica 2 " + rep2Port + ", " + e);
				}
			}

		} else if (inst == NextInst.AFTERMETADATAUPDATE) { //update the two successor nodes, and remove data from the third successor node
			String curNodeName = this.host + ":" + this.port;
			IECSNode succ1Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(curNodeName));
			String succ1NodeName = succ1Node.getNodeHost() + ":" + succ1Node.getNodePort();
			IECSNode succ2Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ1NodeName));
			String succ2NodeName = succ2Node.getNodeHost() + ":" + succ2Node.getNodePort();
			IECSNode succ3Node = this.hashRing.getSuccessor(this.hashRing.getHashVal(succ2NodeName));

			if (succ1Node.getNodePort() != this.port) {
				SuccDataUpdater succ1Updater = new SuccDataUpdater(succ1Node, false, this.fileDb, this.host, this.port);
				Thread updaterThread1 = new Thread(succ1Updater);
                updaterThread1.start();

				//send responsible keys to the successor
				SubscriberSender subSender = new SubscriberSender(succ1Node, listenerMap, hashRing);
				Thread subSenderThread = new Thread(subSender);
				subSenderThread.start();
			}

			if (succ2Node.getNodePort() != this.port && succ2Node.getNodePort() != succ1Node.getNodePort()) {
				SuccDataUpdater succ2Updater = new SuccDataUpdater(succ2Node, false, this.fileDb, this.host, this.port);
				Thread updaterThread2 = new Thread(succ2Updater);
				updaterThread2.start();
			}

			if (succ3Node.getNodePort() != this.port && succ3Node.getNodePort() != succ1Node.getNodePort() && succ3Node.getNodePort() != succ2Node.getNodePort()) {
				SuccDataUpdater succ3Updater = new SuccDataUpdater(succ3Node, true, this.fileDb, this.host, this.port);
				Thread updaterThread3 = new Thread(succ3Updater);
				updaterThread3.start();
			}

		} else {
			logger.error("unhandled next instruction at server: " + inst.toString());
		}
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
