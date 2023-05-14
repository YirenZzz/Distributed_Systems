package shared.communication;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import shared.messages.HandleStrResponse;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

import org.apache.log4j.Logger;


/*
 * Sends to nodes:
 * addreplica <curNode_ip>:<curNode_port> //sends keys that the current node is responsible for
 * removereplica <to_del_rep_ip>:<to_del_rep_port> //delete portion of keys in a replica at the second argument
 * 
 */
public class NodeDataUpdater implements Runnable {
	private String cmd;
	private String rcvNodeIP; 
	private int rcvNodePort;
	private String nbrNodeAddr; //<ip>:<port>
	private static Logger logger = Logger.getRootLogger();

	/**
	 * @param cmd     		command (addreplica or removereplica)
	 * @param rcvNodeAddr	receiver node ip
	 * @param rcvNodePort 	receiver node port
	 * @param nbrNodeAddr	neighbour node address
	 */
	public NodeDataUpdater(String cmd, String rcvNodeIP, int rcvNodePort, String nbrNodeAddr) {
		this.cmd = cmd;
		this.rcvNodeIP = rcvNodeIP;
		this.rcvNodePort = rcvNodePort;
		this.nbrNodeAddr = nbrNodeAddr;
	}

	public void run() {
		CommClient client = new CommClient(rcvNodeIP, rcvNodePort);
		try{
			client.connect();
			String strMsg = cmd + " " + nbrNodeAddr;
			client.sendMessage(new TextMessage(strMsg));
			TextMessage clientRes = client.receiveMessage();
			logger.info("NodeDataUpdater received: " + clientRes.getMsg());
			client.disconnect();

		} catch (Exception e) {
			logger.error ("NodeDataUpdater failed to send message to node " + rcvNodePort + ": " + e);
		}
	}
}


