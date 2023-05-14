package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

//import shared.messages.TextMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

import org.apache.log4j.*;
import java.util.*;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "Sever> ";
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer server;

	private int MAX_KEY_LEN = 20;
	private int MAX_VAL_LEN = 122880; //120kB
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server ) {
		this.clientSocket = clientSocket;
		this.server = server;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			sendMessage(new KVMessageObject(
				KVMessage.StatusType.CONNECTED,
				"Connection to database server established: ",
				null));
			
			while(isOpen) {
				try {
					KVMessage latestMsg = receiveMessage();
					if (latestMsg == null) {
						logger.error("latestMsg is null");
						sendMessage(new KVMessageObject(StatusType.CONNECTION_LOST, null, null));
					} else {
						sendMessage(latestMsg);
					}
					
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */

	public void sendMessage(KVMessage kvmsg) throws IOException {
		logger.info("Sending message");
		if (kvmsg.getStatus() != null) {
			logger.info("Status=" + kvmsg.getStatus().toString());
		}
		logger.info("key=" + kvmsg.getKey() + "value=" + kvmsg.getValue());

		//format: status key value
		String msg = kvmsg.getStatus().toString();
		if (kvmsg.getKey() != null) {
			msg = msg + " " + kvmsg.getKey();
		}
		if (kvmsg.getValue() != null) {
			msg = msg + " " + kvmsg.getValue();
		}

		TextMessage textMsg = new TextMessage(msg);
		byte[] textMsgBytes = textMsg.getMsgBytes();

		output.write(textMsgBytes, 0, textMsgBytes.length); 
		output.flush();

		logger.info("SEND \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ textMsg.getMsg() 
					+"'");	
    }
	
	
	private KVMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		// while(read != 13  &&  read != 10 && read !=-1 && reading) {/* CR, LF, error */
		while(read != 10 && read !=-1 && reading) {/* CR, LF, error */

			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) { //todo: support longer string?
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}

		if (read == -1) {
			return null;
		}
		
		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		String textMsgStr = msg.getMsg();

		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");

		String[] tokens = msg.getMsg().trim().split("\\s+");
		
	    if (tokens[0].equals("get")) {
			logger.info("Processing get request");
			String key = tokens[1];

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				return new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
			}

			if (tokens.length > 2) {
				logger.warn("GET_ERROR: invalid number of arguments");
				return new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
			}

			String value = null;		
			try {
				value = server.getKV(key);
			} catch (Exception e) {
				logger.warn("GET_ERROR" + e);
				return new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
			}

			if (value != null){
				logger.info("value is not null");
				return  new KVMessageObject(KVMessage.StatusType.GET_SUCCESS, key, value);
			} else { //todo: additioal error message for key not present
				logger.info("value is null");
				return new KVMessageObject(KVMessage.StatusType.GET_ERROR, key, null);
			}

		} else if (tokens[0].equals("put")) {	
			String key = tokens[1];

			//Check length of key
			if (key.getBytes().length > MAX_KEY_LEN) {
				logger.error("FAILED: key is too long " + tokens[1]);
				return new KVMessageObject(KVMessage.StatusType.FAILED, "key is too long " + tokens[1], null);
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
					return new KVMessageObject(KVMessage.StatusType.FAILED, key, "value is too long");
				}
			}

			KVMessage.StatusType sType = StatusType.PUT_SUCCESS;
			
			//Initialize return status type
			if (value == null || value == "" || value.equals("null")) {
				sType = StatusType.DELETE_SUCCESS;
				if (!server.inCache(key) && !server.inStorage(key)) {
					logger.info("put val is null, and key is not in storage");
					sType = StatusType.DELETE_ERROR;
					return new KVMessageObject(sType, key, value);
				}
			} else if (server.inCache(key)) {
				sType = StatusType.PUT_UPDATE;
			} else if (server.inStorage(key)) {
				sType = StatusType.PUT_UPDATE;
			}

			try {
				server.putKV(key, value);
			} catch (Exception e) {
				logger.error("Exception in put: " + e);
				if (sType == StatusType.DELETE_SUCCESS) {
					sType = StatusType.DELETE_ERROR;
				} else {
					sType = StatusType.PUT_ERROR;
				}
			}

			return new KVMessageObject(sType, key, value);

		} else {
			logger.warn("Unknown command at server" + msg.getMsg().trim()); //todo: null value causes exception in sendMessage
			return new KVMessageObject(StatusType.FAILED, "unknown command", null);
		}    
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
}

