package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Map;
import java.lang.Math;
import java.security.MessageDigest;

import org.apache.log4j.Logger;

import ecs.IECSNode;
import shared.HashRing;
import shared.communication.CommClient;
import shared.communication.CommServer;
import shared.communication.IServerObject;
import shared.messages.HandleStrResponse;
import shared.messages.HandleStrResponse.NextInst;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;


/*
 * For communication with server
 */
public class KVStore implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private CommClient commClient; //default server to connect with
	private String commClientAddr;
	private int commClientPort;
	private HashRing hashRing;
	private boolean connectedToDefaultServer;
	private int listenPortNum;
	private Map<String, Integer> listenKeyMap;

	/*
	 * listen to subscribe responses
	 * metadataupdate <metadata>
	 */
	private class SubscriptionListener implements Runnable, IServerObject {
		private int serverPort;
		private Logger logger;

		/**
		 * @param serverPort
		 */
		public SubscriptionListener(int serverPort) {
			this.serverPort = serverPort;
			this.logger = Logger.getRootLogger();
		}

		public void run() {
			//start a kvServer
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(serverPort);
				logger.info("CURRENT serverport" + Integer.toString(serverPort));
			} catch (Exception e) {
				logger.error("Unable to create socket: " + e);
				return;
			}

			while(true){
				logger.info("current serverport while loop" + Integer.toString(serverPort));
				try {
					Socket client = serverSocket.accept();         
					logger.info("accepted connection");       
					CommServer connection = 
							new CommServer(client, this);
					
					Thread client_thread = new Thread(connection);
					client_thread.start();
					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
					
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection for port " + serverPort + " \n" + e.getMessage());
				}
			}
		}

		/**
		 * Handle the message 
		 */
		public HandleStrResponse handleStrMsg(String strMsg){
			System.out.println("Subscription message: " + strMsg);
			KVMessage resp = new KVMessageObject(KVMessage.StatusType.SUB_INFO_RECEIVED, "subscribeMsgAck", null);
            return new HandleStrResponse(NextInst.NOINST, resp);
		}

		/**
		 * Subsequent instructions
		 */
		public void handleNextInst(NextInst inst, String strMsg){
			logger.info("SubscriptionListener handleNextInst");

		}

	}


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String a, int p) {
		this.commClient = new CommClient(a, p); //use commClient to reduce repeated functions
		this.commClientAddr = a;
		this.commClientPort = p;
		this.hashRing = null;
		this.connectedToDefaultServer = false;
		this.listenKeyMap = new HashMap<>();

		//compute a hash for listenPortNum
		int hash = 0;
		String hashStr = a + Integer.toString(p) + Long.toString(System.currentTimeMillis());
		try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = md.digest(hashStr.getBytes());
            for (int i = 0; i < hashBytes.length; i++) {
                hash = (hash << 8) + (hashBytes[i] & 0xff);
            }
        } catch (Exception e) {
            logger.error("Exception in computing hash: " + e);
			return;
        }
		this.listenPortNum = Math.abs(hash) % (65500 - 51000) + 51000;	
	}

	@Override
	public void connect() throws Exception {
		this.connectedToDefaultServer = true;
		this.commClient.connect();
	}

	@Override
	public void disconnect() {
		this.connectedToDefaultServer = false;
		this.commClient.disconnect();
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if (this.connectedToDefaultServer == false) {
			throw new ConnectException("Default server is not connected"); 
		}
		
		String msg = "put " + key;
		if (value != null && value != "" && value != "null") {
			msg = msg + " " + value;
		}

		//first find the server responsible, if metadata is not null
		CommClient respServerComm = this.commClient;
		boolean isDefaultComm = true;

		if (this.hashRing != null) {
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}
		}

		//connect to the server identified by local meta data
		try {
			respServerComm.connect();
		} catch (ConnectException e) {
			//query each node for the new metadata
			String metaDataResp = queryMetadataFromOtherServers();
			if (metaDataResp == null) {
				logger.warn("All servers are shut down");
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			} 

			//update local hash ring and try to get the new responsible server
			this.hashRing = new HashRing(metaDataResp);
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}

			try {
				respServerComm.connect();
			} catch (Exception ex) {
				logger.error("Exception in connecting to updated server: " + ex);
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}
		}

		//initial attempt to update the key
		respServerComm.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = respServerComm.receiveMessage();
		logger.info("Received message in put: " + responseMsg.getMsg().trim());
		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) { //maintain the default connection to server
			respServerComm.disconnect();
		}

		//return the message directly if meta data does not need to be updated
		if (responseKVMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			return responseKVMsg;
		}

		//request keyrange from responsible server
		respServerComm.connect();
		respServerComm.sendMessage(new TextMessage("keyrange"));
		responseMsg = respServerComm.receiveMessage();
		logger.info("Received response from keyrange in put: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) { 
			respServerComm.disconnect();
		}

		//update local hash ring structure
		this.hashRing = new HashRing(responseKVMsg.getKey()); //keyrange is stored as the key, assuming result is not null
		//get the server responsible for this key
		IECSNode updatedRespServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

		CommClient updatedRespServerComm = new CommClient(updatedRespServer.getNodeHost(), updatedRespServer.getNodePort());
		updatedRespServerComm.connect();

		//try with new metadata
		updatedRespServerComm.sendMessage(new TextMessage(msg));
		responseMsg = updatedRespServerComm.receiveMessage();
		logger.info("Received messages after retry, in put: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		updatedRespServerComm.disconnect();

		return responseKVMsg; //assume this is not SERVER_NOT_RESPONSIBLE
	}

	@Override
	public KVMessage get(String key) throws Exception {
		String msg = "get " + key;

		//first find the server responsible, if metadata is not null
		CommClient respServerComm = this.commClient;
		boolean isDefaultComm = true;

		//get the coordinator node of the key
		if (this.hashRing != null) {
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}
		}

		//connect to the server identified by local meta data
		try {
			respServerComm.connect();
		} catch (ConnectException e) {
			//query each node for the new metadata
			logger.info("Cannot connect to server");
			String metaDataResp = queryMetadataFromOtherServers();
			if (metaDataResp == null) {
				logger.warn("All servers are shut down");
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			} 

			//update local hash ring and try to get the new responsible server
			this.hashRing = new HashRing(metaDataResp);
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}

			try {
				respServerComm.connect();
			} catch (Exception ex) {
				logger.error("Exception in connecting to updated server: " + ex);
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}
		}

		//initial attempt to get key
		respServerComm.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = respServerComm.receiveMessage();
		logger.info("Received message in get: " + responseMsg.getMsg().trim());

		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) { //maintain the default connection to server
			respServerComm.disconnect();
		}
		
		//return the message directly if meta data does not need to be updated
		if (responseKVMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			return responseKVMsg;
		}

		//request keyrange from responsible server
		respServerComm.connect();
		respServerComm.sendMessage(new TextMessage("keyrange"));
		responseMsg = respServerComm.receiveMessage();
		logger.info("Received response from keyrange in get: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) {
			respServerComm.disconnect();
		}

		//update local hash ring structure
		this.hashRing = new HashRing(responseKVMsg.getKey()); //kvmessage is stored as the key, assuming result is not null

		//get the server responsible for this key
		IECSNode updatedRespServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

		CommClient updatedRespServerComm = new CommClient(updatedRespServer.getNodeHost(), updatedRespServer.getNodePort());
		updatedRespServerComm.connect();

		//try with new metadata
		updatedRespServerComm.sendMessage(new TextMessage(msg));
		responseMsg = updatedRespServerComm.receiveMessage();
		logger.info("Received messages after retry, in get: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		updatedRespServerComm.disconnect();

		return responseKVMsg; //assume this is not SERVER_NOT_RESPONSIBLE
	}

	/*
	 * Filter the keys according to regex pattern
	 */
	private KVMessage filterResKeys(String strRespMsg, String regexPattern) { 
		//strRespMsg format is GETALLKEYS_SUCCESS <key1>,<key2>,<key3>
		logger.info("filterResKeys input strRespMsg: " + strRespMsg + ", regexPattern: " + regexPattern);
		if (regexPattern == null) {
			return new KVMessageObject(strRespMsg);
		}
		String[] arrRespMsg = strRespMsg.split("\\s+");
		if (arrRespMsg.length < 2) {
			return new KVMessageObject(strRespMsg);
		}

		//check regex
		Pattern pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
		String[] inputList = arrRespMsg[1].split(",");
		List<String> filteredList = new ArrayList<>();

		for (String msg : inputList) {
			Matcher matcher = pattern.matcher(msg);
			boolean matches = matcher.matches();
			if (matches) {
				filteredList.add(msg);
			}
		}

		String strFilteredList = String.join(",", filteredList);
		String strRes = arrRespMsg[0] + " " + strFilteredList;
		return new KVMessageObject(strRes);
	}
		
	/*
	 * Get all keys
	 */
	public KVMessage getAllKeys(String regexPattern) throws Exception {
		String msg = "getAllKeys";

		//choose a random server to query from
		Random r = new Random();
		char randChar = (char)(r.nextInt(26) + 'a');
		String key = String.valueOf(randChar);

		//first find the server responsible, if metadata is not null
		CommClient respServerComm = this.commClient;
		boolean isDefaultComm = true;

		//get the coordinator node of the key
		if (this.hashRing != null) {
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}
		}

		//connect to the server identified by local meta data
		try {
			respServerComm.connect();
		} catch (ConnectException e) {
			//query each node for the new metadata
			logger.info("Cannot connect to server");
			String metaDataResp = queryMetadataFromOtherServers();
			if (metaDataResp == null) {
				logger.warn("All servers are shut down");
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}

			//update local hash ring and try to get the new responsible server
			this.hashRing = new HashRing(metaDataResp);
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}

			try {
				respServerComm.connect();
			} catch (Exception ex) {
				logger.error("getAllKeys exception in connecting to updated server: " + ex);
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}
		}

		//initial attempt to get key
		respServerComm.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = respServerComm.receiveMessage();
		String strRespMsg = responseMsg.getMsg().trim();
		logger.info("Received message in getAllKeys: " + strRespMsg);

		KVMessage responseKVMsg = filterResKeys(strRespMsg, regexPattern);
		
		if (!isDefaultComm) { //maintain the default connection to server
			respServerComm.disconnect();
		}
		
		//return the message directly if meta data does not need to be updated //TODO: regex if not null
		if (responseKVMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			return responseKVMsg;
		}

		//request keyrange from responsible server
		respServerComm.connect();
		respServerComm.sendMessage(new TextMessage("keyrange"));
		responseMsg = respServerComm.receiveMessage();
		logger.info("Received response from keyrange in getAllKeys: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) {
			respServerComm.disconnect();
		}

		//update local hash ring structure
		this.hashRing = new HashRing(responseKVMsg.getKey()); //kvmessage is stored as the key, assuming result is not null

		//get the server responsible for this key
		IECSNode updatedRespServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key)); //todo: connect to any key

		CommClient updatedRespServerComm = new CommClient(updatedRespServer.getNodeHost(), updatedRespServer.getNodePort());
		updatedRespServerComm.connect();

		//try with new metadata
		updatedRespServerComm.sendMessage(new TextMessage(msg));
		responseMsg = updatedRespServerComm.receiveMessage();
		logger.info("Received messages after retry, in getAllKeys: " + responseMsg.getMsg().trim());
		responseKVMsg = filterResKeys(strRespMsg, regexPattern);
		updatedRespServerComm.disconnect();

		return responseKVMsg; //assume this is not SERVER_NOT_RESPONSIBLE
	}

	/*
	 * get key range of servers
	 */
	public KVMessage keyRange() throws Exception {
		String msg = "keyrange";
		commClient.connect();//todo: remove?
		commClient.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = commClient.receiveMessage();
		logger.info("Received messages, in keyrange: " + responseMsg.getMsg().trim());

		//handle server_stopped
		if (responseMsg.getMsg().split("\\s+")[0].equals("SERVER_STOPPED")) {
			// responseMsg = 
			KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
			return responseKVMsg;
		}

		String respMsgMetadata = responseMsg.getMsg().split("\\s+")[1];

		//append node name to each metadata, and update local hash ring structure
		String strMetaData = "";
		String[] arrMsgMetadata = respMsgMetadata.split(";");

		for (String md : arrMsgMetadata) {
			String[] vals = md.split(",");
			strMetaData = strMetaData + vals[2] + "," + vals[0] + "," + vals[1] + "," + vals[2] + ";";
		}

		this.hashRing = new HashRing(strMetaData);
		
		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		return responseKVMsg;
	}

	public KVMessage keyRangeRead() throws Exception {
		String msg = "keyrange_read";
		commClient.connect();//todo: remove?
		commClient.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = commClient.receiveMessage();
		logger.info("Received messages, in keyrange_read: " + responseMsg.getMsg().trim());

		//handle server_stopped
		if (responseMsg.getMsg().split("\\s+")[0].equals("SERVER_STOPPED")) {
			KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
			return responseKVMsg;
		}

		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		return responseKVMsg;
	}

	public KVMessage beginTX() throws Exception {
		String msg = "beginTX";
		commClient.connect();
		commClient.sendMessage(new TextMessage(msg));
		
		//receive server response
		TextMessage responseMsg = commClient.receiveMessage();
		logger.info("Received messages, in beginTX: " + responseMsg.getMsg().trim());

		//handle server_stopped
		if (responseMsg.getMsg().split("\\s+")[0].equals("SERVER_STOPPED")) {
			KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
			return responseKVMsg;
		}
		
		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		return responseKVMsg;
	}

	public KVMessage commitTX(String LogList, String transactionId) throws Exception {
		if (LogList == null) {
			LogList = "put,a,b";
		}
		if (transactionId == null) {
			transactionId = "1";
		}
		String msg = "commitTX " + transactionId +" "+LogList;
		commClient.connect();
		commClient.sendMessage(new TextMessage(msg));
		
		//receive server response
		TextMessage responseMsg = commClient.receiveMessage();
		logger.info("Received messages, in commitTX: " + responseMsg.getMsg().trim());

		//handle server_stopped
		if (responseMsg.getMsg().split("\\s+")[0].equals("SERVER_STOPPED")) {
			KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
			return responseKVMsg;
		}

		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		return responseKVMsg;
	}

	/*
	 * Subscribe to keys that match the regex pattern
	 */
	public KVMessage subscribeKey (String keyRegex) throws Exception {
		KVMessage allKeysRes = getAllKeys(keyRegex);
		if (allKeysRes.getKey() == null) {
			System.out.println("No matching key");
			return new KVMessageObject(KVMessage.StatusType.SUB_ERROR, null, null);
		}
		String[] listKeys = allKeysRes.getKey().split(",");
		if (listKeys.length == 0) {
			System.out.println("No matching key");
			return new KVMessageObject(KVMessage.StatusType.SUB_ERROR, null, null);
		}

		for (String key : listKeys) {
			System.out.print("Subscription result for key \"" + key + "\": ");
			KVMessage curRes = subscribeOneKey(key);
			System.out.print(curRes.getStatus());
			if (curRes.getKey() != null && curRes.getStatus() != KVMessage.StatusType.SUB_SUCCESS) {
				System.out.println(", " + curRes.getKey());
			} else {
				System.out.println("");
			}
		}
		
		return new KVMessageObject(KVMessage.StatusType.SUB_SUCCESS, null, null);
	}

	/*
	 * Subscribe to a key 
	 */
	public KVMessage subscribeOneKey (String key) throws Exception {
		//check if already subscribed
		if (listenKeyMap.containsKey(key)) {
			return new KVMessageObject(KVMessage.StatusType.SUB_ERROR,  "already Subscribed", null);
		}

		//find an available port
		int portNum = this.listenPortNum;
		this.listenPortNum = this.listenPortNum + 1;
		logger.info("Available port: " + portNum);

		//start listener thread
		SubscriptionListener listener = new SubscriptionListener(portNum);
		Thread listenerThread = new Thread(listener);
		listenerThread.start();

		String msg = "sub " + portNum + " " + key;

		//first find the server responsible, if metadata is not null
		CommClient respServerComm = this.commClient;
		boolean isDefaultComm = true;
		if (this.hashRing != null) {
			logger.info("Subscribe command, HASHRING is not null");
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

			if (respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}
		}

		//connect to the server identified by local meta data
		try {
			respServerComm.connect();
		} catch (ConnectException e) {
			//query each node for the new metadata
			logger.info("Cannot connect to server");
			String metaDataResp = queryMetadataFromOtherServers();
			if (metaDataResp == null) {
				logger.warn("All servers are shut down");
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			} 

			//update local hash ring and try to get the new responsible server
			this.hashRing = new HashRing(metaDataResp);
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}

			try {
				respServerComm.connect();
			} catch (Exception ex) {
				logger.error("Exception in connecting to updated server: " + ex);
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}
		}

		//initial attempt to subscribe to the key
		respServerComm.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = respServerComm.receiveMessage();
		logger.info("Received message in subscribe: " + responseMsg.getMsg().trim());

		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) { //maintain the default connection to server
			respServerComm.disconnect();
		}
		
		//return the message directly if meta data does not need to be updated
		if (responseKVMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			if (responseKVMsg.getStatus() == KVMessage.StatusType.SUB_SUCCESS) {
				listenKeyMap.put(key, portNum);
			}
			return responseKVMsg;
		}

		//request keyrange from responsible server
		respServerComm.connect();
		respServerComm.sendMessage(new TextMessage("keyrange"));
		responseMsg = respServerComm.receiveMessage();
		logger.info("Received response from keyrange in subscribe: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) {
			respServerComm.disconnect();
		}

		//update local hash ring structure
		this.hashRing = new HashRing(responseKVMsg.getKey()); //kvmessage is stored as the key, assuming result is not null

		//get the server responsible for this key
		IECSNode updatedRespServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

		CommClient updatedRespServerComm = new CommClient(updatedRespServer.getNodeHost(), updatedRespServer.getNodePort());
		updatedRespServerComm.connect();

		//try with new metadata
		updatedRespServerComm.sendMessage(new TextMessage(msg));
		responseMsg = updatedRespServerComm.receiveMessage();
		logger.info("Received messages after retry, in subscribe: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		updatedRespServerComm.disconnect();

		if (responseKVMsg.getStatus() == KVMessage.StatusType.SUB_SUCCESS) {
			listenKeyMap.put(key, portNum);
		}

		return responseKVMsg;
	}

	/*
	 * helper function to unsubscribe to a key 
	 */
	public KVMessage unsubscribeKey (String key) throws Exception {
		//check if key subscribed
		if (!listenKeyMap.containsKey(key)) {
			return new KVMessageObject(KVMessage.StatusType.UNSUB_ERROR, "Not Yet Subscribed", null);
		}
		int portNum = listenKeyMap.get(key);
		String msg = "unsub " + portNum + " " + key;

		//first find the server responsible, if metadata is not null
		CommClient respServerComm = this.commClient;
		boolean isDefaultComm = true;
		if (this.hashRing != null) {
			logger.info("Unsubscribe command, HASHRING is not null");
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

			if (respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}
		}

		//connect to the server identified by local meta data
		try {
			respServerComm.connect();
		} catch (ConnectException e) {
			//query each node for the new metadata
			logger.info("Cannot connect to server");
			String metaDataResp = queryMetadataFromOtherServers();
			if (metaDataResp == null) {
				logger.warn("All servers are shut down");
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			} 

			//update local hash ring and try to get the new responsible server
			this.hashRing = new HashRing(metaDataResp);
			IECSNode respServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));
			if (respServer.getNodeHost() != this.commClientAddr || respServer.getNodePort() != this.commClientPort) {
				isDefaultComm = false;
				respServerComm = new CommClient(respServer.getNodeHost(), respServer.getNodePort());
			}

			try {
				respServerComm.connect();
			} catch (Exception ex) {
				logger.error("Exception in connecting to updated server: " + ex);
				return new KVMessageObject(StatusType.SERVER_STOPPED, key, null);
			}
		}

		//initial attempt to unsubscribe to the key
		respServerComm.sendMessage(new TextMessage(msg));

		//receive server response
		TextMessage responseMsg = respServerComm.receiveMessage();
		logger.info("Received message in unsubscribe: " + responseMsg.getMsg().trim());

		KVMessage responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) { //maintain the default connection to server
			respServerComm.disconnect();
		}
		
		//return the message directly if meta data does not need to be updated
		if (responseKVMsg.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
			if (responseKVMsg.getStatus() == StatusType.UNSUB_SUCCESS) {
				listenKeyMap.remove(key);
			}
			return responseKVMsg;
		}

		//request keyrange from responsible server
		respServerComm.connect();
		respServerComm.sendMessage(new TextMessage("keyrange"));
		responseMsg = respServerComm.receiveMessage();
		logger.info("Received response from keyrange in unsubscribe: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		if (!isDefaultComm) {
			respServerComm.disconnect();
		}

		//update local hash ring structure
		this.hashRing = new HashRing(responseKVMsg.getKey()); //kvmessage is stored as the key, assuming result is not null

		//get the server responsible for this key
		IECSNode updatedRespServer = hashRing.getSuccessorInclusive(hashRing.getHashVal(key));

		CommClient updatedRespServerComm = new CommClient(updatedRespServer.getNodeHost(), updatedRespServer.getNodePort());
		updatedRespServerComm.connect();

		//try with new metadata
		updatedRespServerComm.sendMessage(new TextMessage(msg));
		responseMsg = updatedRespServerComm.receiveMessage();
		logger.info("Received messages after retry, in unsubscribe: " + responseMsg.getMsg().trim());
		responseKVMsg = new KVMessageObject(responseMsg.getMsg().trim());
		updatedRespServerComm.disconnect();

		if (responseKVMsg.getStatus() == StatusType.UNSUB_SUCCESS) {
			listenKeyMap.remove(key);
		}
		return responseKVMsg;
	}


	/*
	 * 	To deal with the cases SERVER_STOPPED or server is already shut down
	 */
	private String queryMetadataFromOtherServers () {
		if (this.hashRing == null) {
			logger.info("hash ring is null");
			return null;
		}
	
		for (Map.Entry<String,IECSNode> entry : this.hashRing.nodeMap.entrySet()) {
			IECSNode curNode = entry.getValue();
			CommClient curCommClient = new CommClient(curNode.getNodeHost(), curNode.getNodePort());

			try {
				curCommClient.connect();
			} catch (ConnectException e) {
				logger.warn("Server on port " + curNode.getNodePort() + "is shut down");
				continue;
			} catch (Exception e) {
				logger.error("Error connecting to server on port" + curNode.getNodePort());
				continue;
			}

			try {
				curCommClient.sendMessage(new TextMessage("keyrange"));
				TextMessage responseMsg = curCommClient.receiveMessage();
				logger.info("Received message in queryMetadataFromOtherServers: " + responseMsg.getMsg().trim());
				curCommClient.disconnect();

				String msg = responseMsg.getMsg();
				String metadataRes = msg.split("\\s+")[1];
				String res = "";
				String[] arrMsgMetadata = metadataRes.split(";");

				for (String md : arrMsgMetadata) {
					String[] vals = md.split(",");
					res = res + vals[2] + "," + vals[0] + "," + vals[1] + "," + vals[2] + ";";
				}
				return res;

			} catch (Exception e) {
				logger.warn("Exception in queryMetadataFromOtherServers sendMsg to" + curNode.getNodePort());
				continue;
			}
		}
		//none of the servers is available
		return null;
	}
	

	public boolean isRunning() {
		return commClient.isRunning();
	}
	
	public void setRunning(boolean run) {
		commClient.setRunning(run);
	}
}
