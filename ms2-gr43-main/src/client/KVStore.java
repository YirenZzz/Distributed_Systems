package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.log4j.Logger;

import ecs.IECSNode;
import shared.HashRing;
import shared.communication.CommClient;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

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
