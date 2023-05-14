package shared.communication;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import shared.messages.HandleStrResponse;
import shared.messages.KVMessage;
import shared.messages.KVMessageObject;
import shared.messages.TextMessage;
import shared.messages.HandleStrResponse.NextInst;
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
public class CommServer implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "Sever> ";
	public boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private IServerObject server;

	private int MAX_KEY_LEN = 20;
	private int MAX_VAL_LEN = 122880; //120kB
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public CommServer(Socket clientSocket, IServerObject server ) {
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
				"Connection to server is established",
				null));
			
			while(isOpen) {
				try {
					HandleStrResponse latestResp = receiveMessage();
					if (latestResp == null) {
						logger.error("latestResp is null");
						sendMessage(new KVMessageObject(StatusType.CONNECTION_LOST, null, null));
					} else {
						sendMessage(latestResp.responseMessage); //send the response to client
						//call the server to process subsequent instructions
						if (latestResp.nextInst != NextInst.NOINST) {
							this.server.handleNextInst(latestResp.nextInst, latestResp.nextInstParam); //parameter is a string
						}
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
	

	private HandleStrResponse receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
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

        return this.server.handleStrMsg(textMsgStr);
    }
}
