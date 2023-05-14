package app_kvClient;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.*;
import shared.messages.KVMessage.StatusType;

/*
 * Handle Client Command
 */
public class KVClient implements IKVClient {
    private Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "Client> ";
    private BufferedReader stdin;
    private KVStore store = null; //assuming client can only connect to 1 host
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        logger.info("Connecting. Host: " + hostname + " Port: " + port);
        store = new KVStore(hostname, port);
        store.connect();
        logger.info ("Connection created");
    }

    @Override
    public KVCommInterface getStore(){
        return store;
    }

    /**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
     * @param
	 */
    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

    /*
     * Process command line input
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
					System.out.println("Connection Established");
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				} catch (Exception e) {
                    printError("Exception in connect!");
                    logger.warn("Exception in connect!");
                }
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else  if (tokens[0].equals("get")) {

			if(tokens.length < 2) {
				printError("No key passed!"); //todo: exception?
			} else if (tokens.length > 2) {
				printError("Too many arguments for get!");
			} else if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.get(tokens[1]);

					//print out the value indexed by the given key, or an error message
					if (response.getStatus() == StatusType.GET_SUCCESS) { 
						System.out.println(response.getValue());
					} else {
						printError("Failed to get key " + tokens[1]);
					}
				} catch (Exception e) {
					printError("Exception in get: " + e);
				}

			} else {
				printError("Not connected!");
			}
		} else if (tokens[0].equals("put")) {
			//Combine value entry
			String val = null;
			if (tokens.length > 2) {
				val = tokens[2];
				int tokenIdx = 3;
				while (tokens.length > tokenIdx) {
					val = val + " " + tokens[tokenIdx];
					tokenIdx++;
				}
			}
			
			if(tokens.length < 2) {
				printError("No key passed!"); //todo: exception?
			} else if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.put(tokens[1], val);

					// print out the notification if the put operation was successful or not
					if (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE ||
							response.getStatus() == StatusType.DELETE_SUCCESS) { 
						System.out.println("SUCCESS");
					} else {
						System.out.println("ERROR");
					}
				} catch (Exception e) {
					printError("Exception in get: " + e);
				}

			} else {
				printError("Not connected!");
			}
			
		}else if(tokens[0].equals("disconnect")) {
			disconnect();
			System.out.println("Disconnected from the server");
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp(); //todo: use this line
		}
    }

    /*
     * Call KVStore to send message
     */
    private void sendMessage(String msg){
		try {
			store.sendMessage(new TextMessage(msg));
		} catch (IOException e) {
			printError("Unable to send message!");
			disconnect();
		}
	}

	private void connect(String address, int port) throws Exception {
		store = new KVStore(address, port);
		store.connect();
	}
	
	private void disconnect() {
		if(store != null) {
			store.disconnect();
			store = null;
		}
	}

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t get the value associated with the given key from the database \n");
        sb.append(PROMPT).append("put <key>");
		sb.append("\t\t delete the input key from the database \n");
        sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t set the value of the given key \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

    private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

    private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

     /**
     * Main function for the client
     * @param args
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.ALL);
			KVClient client = new KVClient();
		    client.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
