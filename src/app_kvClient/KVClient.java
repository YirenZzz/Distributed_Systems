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
import shared.communication.CommClient;
import shared.messages.*;
import shared.messages.KVMessage.StatusType;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	private Map<String, List<String>> transactionLogs = new HashMap<>(); // transaction logs
	private Map<String, String> transactionInitVals = new HashMap<>(); // watch on all values
	private String transactionId = null;
	private boolean beginTX = false;
	private List<String> prevCommands = new ArrayList<>();

	/*
	 * Query value of a key
	 */
	private class ValueQuerier implements Callable<String[]> {
		private String curKey;
		private String serverIP; 
		private int serverPort;

		public ValueQuerier(String serverIP, int serverPort, String curKey) {
			this.serverIP = serverIP;
			this.serverPort = serverPort;
			this.curKey = curKey;
		}

		@Override
		public String[] call() {
			// get value of the current key 
			CommClient curCl = new CommClient(serverIP, serverPort);
			String strRespMsg = null;
			String val = null;

            try{
                curCl.connect();
                curCl.sendMessage(new TextMessage("get " + curKey));
                TextMessage respMsg  = curCl.receiveMessage();
				strRespMsg = respMsg.getMsg();
                logger.info("ValueQuerier received: " + strRespMsg);
                curCl.disconnect();

			} catch (Exception e) {
				logger.error ("ValueQuerier failed to send message to node " + Integer.toString(serverPort) + ": " + e);
				return null;
			}

			KVMessage resp = new KVMessageObject(strRespMsg);
			if (resp.getStatus() == StatusType.GET_SUCCESS) { 
				val = resp.getValue();
			} else {
				printError("Key " + curKey + " does not exist");
				return null;
			}
			String[] res = {curKey, val};
			return res;
		}
	}

	/*
	 * Execute rollback operation
	 */
	private class RollbackExecuter implements Runnable {
		private String serverIP;
		private int serverPort;
        private String key;
		private String value;

		public RollbackExecuter(String serverIP, int serverPort, String key, String value) {
			this.serverIP = serverIP;
			this.serverPort = serverPort;
            this.key = key;
			this.value = value;
		}

		public void run() {
			KVStore cl = new KVStore(serverIP, serverPort);
            try{
                cl.connect();
				cl.put(key, value);
                cl.disconnect();
			} catch (Exception e) {
				logger.error ("RollbackExecuter failed to put key " + key + ": " + e);
				//retry
				try {
					cl.connect();
					cl.put(key, value);
					cl.disconnect();
				} catch (Exception retryException) {
					logger.error ("RollbackExecuter retry failed to put key " + key + ": " + retryException);
				}
			}
		}
	}


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
		Pattern pattern = Pattern.compile("/\\d+", Pattern.CASE_INSENSITIVE);
		String nextCmd = "";
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT + nextCmd);
			
			try {
				String cmdLine = stdin.readLine();
				String curCmd = nextCmd + cmdLine;
				String cmd = curCmd.split("\\s+")[0];
				Matcher matcher = pattern.matcher(cmd);

				if (matcher.matches()) { //use a previous command 
					if (prevCommands.size() == 0) {
						continue;
					}
					
					String strIdx = cmd.substring(1);
					int cmdIdx = Integer.parseInt(strIdx);
					nextCmd = prevCommands.get(prevCommands.size() - cmdIdx);

				} else {
					this.handleCommand(curCmd);
					nextCmd = "";

					if (cmd.trim().length() > 0) {
						prevCommands.add(cmd);
					}
				}
				
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
		//String //TODO

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
			
		} else if (tokens[0].equals("get")) {

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
				printError("No key passed!");
			} else if (store != null && store.isRunning()) {
				if (beginTX==false){
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
				}
				else{
					List<String> OperationList = transactionLogs.get(transactionId);
					OperationList.add("put,"+tokens[1]+","+val);
					transactionLogs.put(transactionId, OperationList);
					System.out.println("QUEUED");
				}

			} else {
				printError("Not connected!");
			}
			
		} else if (tokens[0].equals("keyrange")) {
			if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.keyRange();
					if (response.getStatus() == StatusType.KEYRANGE_SUCCESS) { 
						System.out.println(response.getKey());
					} else {
						printError("Failed to get keyrange: " + response.getStatus());
					}
				} catch (Exception e) {
					printError("Exception in keyrange: " + e);
				}
			} else {
				printError("Not connected!");
			}

		} else  if (tokens[0].equals("keyrange_read")) {
			if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.keyRangeRead();
					if (response.getStatus() == StatusType.KEYRANGE_READ_SUCCESS) { 
						System.out.println(response.getKey());
					} else {
						printError("Failed to get keyrange_read: " + response.getStatus());
					}
				} catch (Exception e) {
					printError("Exception in keyrange_read: " + e);
				}
			} else {
				printError("Not connected!");
			}
		

		} else if (tokens[0].equals("beginTX")) {
			if (store != null && store.isRunning()) {
				String[] listKeys = null;

				try {
					KVMessage response = store.getAllKeys(null); //regex is null

					//print out the value indexed by the given key, or an error message
					if (response.getStatus() == StatusType.GETALLKEYS_SUCCESS) { 
						String strKeys = response.getKey();
						listKeys = strKeys.split(",");
					} else {
						printError("Failed to set watch on all keys");
						return;
					}

				} catch (Exception e) {
					printError("Exception in beginTX getAllKeys: " + e);
					return;
				}

				//record values of keys
				ExecutorService executor = Executors.newFixedThreadPool(10);
				List<Future<String[]>> resVals = new ArrayList<>(); //first element of return string[] is key, second element is value
				for (String curKey : listKeys) {
					Future<String[]> r = executor.submit(new ValueQuerier(this.serverAddress, this.serverPort, curKey));
					resVals.add(r);
				}

				for (Future<String[]> r : resVals) { //wait for each computation to complete
					try {
						String[] arrRes = r.get();
						if (arrRes == null) { //error during query
							logger.info("beginTX arrRes is null");
							continue;
						}
						transactionInitVals.put(arrRes[0], arrRes[1]);
					} catch (Exception e) {
						logger.warn("Exception in geting res val: " + e);
					}
				}
				logger.info("length of transactionInitVals: " + Integer.toString(transactionInitVals.size()));
				executor.shutdown();

				try {
					KVMessage response = store.beginTX();
					if (response.getStatus() == StatusType.BEGINTX_SUCCESS) { 
						beginTX = true;
						transactionId = response.getValue();
						
						if (!transactionLogs.containsKey(transactionId)) {
							transactionLogs.put(transactionId, new ArrayList<String>());
							System.out.println("Transaction ID is: "+response.getValue());
						}
						
					} else {
						printError("Failed to beginTX: " + response.getStatus());
					}
				} catch (Exception e) {
					printError("Exception in beginTX: " + e);
				}
			} else {
				printError("Not connected!");
			}
		} 
		
		else if (tokens[0].equals("commitTX")) {
			if (store != null && store.isRunning()) {
				beginTX = false;
				
				List<String> operationList = transactionLogs.get(transactionId);
				String result = "";

				//list of operation keys
				List<String> operationKeys = new ArrayList<>();
				for (int i = 0; i < operationList.size(); i++) {
					String operation = operationList.get(i);
					String[] arrOp = operation.split(",");
					operationKeys.add(arrOp[1]);
				}

				//check if any key in the operation list has been modified 
				ExecutorService executor = Executors.newFixedThreadPool(10);
				List<Future<String[]>> resVals = new ArrayList<>(); //first element of return string[] is key, second element is value
				for (int i = 0; i < operationKeys.size(); i++) {
					String curKey = operationKeys.get(i);
					Future<String[]> r = executor.submit(new ValueQuerier(this.serverAddress, this.serverPort, curKey));
					resVals.add(r);
				}

				Map<String, String> curKeyValMap = new HashMap<>();
				for (Future<String[]> r : resVals) { //wait for each computation to complete
					try {
						String[] arrRes = r.get();
						if (arrRes == null) { //error during query
							logger.info("commitTX arrRes is null");
							System.out.println("commitTX error, rolling back");
							return;
						}
						curKeyValMap.put(arrRes[0], arrRes[1]);
					} catch (Exception e) {
						logger.warn("Exception in geting res val: " + e);
					}
				}
				logger.info("length of curKeyValMap: " + Integer.toString(curKeyValMap.size()));
				executor.shutdown();

				Iterator<Map.Entry<String, String>> iter = curKeyValMap.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String, String> entry = iter.next();
					String key = entry.getKey();
					String initVal = transactionInitVals.get(key);
					String curVal = entry.getValue();
					if (!transactionInitVals.containsKey(key) || !initVal.equals(curVal)) {
						//Do not commit is value has changed
						logger.info("commitTX valueChanged");
						System.out.println("commitTX failed: value has been changed for key " + key +
							", initial value is \"" + initVal + "\", current value is \"" + curVal + "\"");
						return; 
					}
				}

				//commit operations
				for (int i = 0; i < operationList.size(); i++) {
					String operation = operationList.get(i);
					result = result + operation + ";";
				}

				try {
					
					KVMessage response = store.commitTX(result, transactionId); // todo
					if (response.getStatus() == StatusType.COMMITTX_SUCCESS) { 
						// transactionId = response.getValue()
						
						// if (!transactionLogs.containsKey(transactionId)) {
						// 	transactionLogs.put(transactionId, new ArrayList<>());
						// 	System.out.println("Transaction ID is: "+response.getValue());
						// }
						System.out.println("CommitTX successful: " + response.getStatus());
						transactionInitVals.clear();
						
					} else {
						//rollback commits by recommitting initial values, with retry
						printError("commitTX failed, rolling back");
						Iterator<Map.Entry<String, String>> initValIter = transactionInitVals.entrySet().iterator();
						while (initValIter.hasNext()) {
							Map.Entry<String, String> initValEntry = initValIter.next();
							RollbackExecuter rbe = new RollbackExecuter(this.serverAddress, this.serverPort, initValEntry.getKey(), initValEntry.getValue());
							Thread rbeThread = new Thread(rbe);
							rbeThread.start();	
						}
					}
				} catch (Exception e) {
					printError("Exception in commitTX: " + e);
				}
			} else {
				printError("Not connected!");
			}
		}

		else if (tokens[0].equals("rollback")) { //abort transaction
			transactionLogs.remove(transactionId);
			transactionId = null;
			beginTX = false;
			System.out.println("Transaction aborted");
		}
		else if (tokens[0].equals("sub")) { //subscribe to a key
			if(tokens.length < 2) {
				printError("No key passed!");
			} else if (tokens.length > 2) {
				printError("Too many arguments for subscribe!");
			} else if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.subscribeKey(tokens[1]);

					// //print out error message if subscribe failed
					// if (response.getStatus() != StatusType.SUB_SUCCESS) { 
					// 	printError("Failed to subscribe to key \"" + tokens[1] + "\": " + response.getKey());
					// } else {
					// 	System.out.println("Subscribed to key \"" + tokens[1] + "\"");
					// }

				} catch (Exception e) {
					printError("Exception in subscribe");
				}

			} else {
				printError("Not connected!");
			}
		} else if (tokens[0].equals("unsub")) { //unsubscibe
			if(tokens.length < 2) {
				printError("No key passed!");
			} else if (tokens.length > 2) {
				printError("Too many arguments for subscribe!");
			} else if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.unsubscribeKey(tokens[1]);

					//print out error message if subscribe failed
					if (response.getStatus() != StatusType.UNSUB_SUCCESS) { 
						printError("Failed to unsubscribe to key " + tokens[1]);
					} else {
						System.out.println("Unsubscribed to key \"" + tokens[1] + "\"");
					}

				} catch (Exception e) {
					printError("Exception in unsubscribe: " + e);
				}

			} else {
				printError("Not connected!");
			}
			
		} else if (tokens[0].equals("getAllKeys")) { 
			//get the value of all keys. If include regex in parameter, will return all keys satisfying the regex pattern
			if (tokens.length > 2) {
				printError("Too many arguments for getAllKeys!");
				return; 
			}

			String regexPattern = null;
			if (tokens.length == 2) {
				regexPattern = tokens[1];
			}

			if (store != null && store.isRunning()) {
				try {
					KVMessage response = store.getAllKeys(regexPattern);

					//print out the value indexed by the given key, or an error message
					if (response.getStatus() == StatusType.GETALLKEYS_SUCCESS) { 
						System.out.println(response.getKey());
					} else {
						printError("Failed to get all keys");
					}

				} catch (Exception e) {
					printError("Exception in subscribe: " + e);
				}

			} else {
				printError("Not connected!");
			}
			
		}  else if (tokens[0].equals("disconnect")) {
			disconnect();
			System.out.println("Disconnected from the server");
			
		} else if (tokens[0].equals("logLevel")) {
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
			
		} else if (tokens[0].equals("help")) {
			printHelp();
		} else if (!tokens[0].equals("")){
			printError("Unknown command");
			printHelp();
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
		sb.append("\t set the value of the given key \n");
		sb.append(PROMPT).append("put <key>");
		sb.append("\t\t delete the input key from the database \n");
		sb.append(PROMPT).append("sub <regex_key>");
		sb.append("\t\t subscribe to a key, supports specification using regular expression \n");
		sb.append(PROMPT).append("unsub <regex_key>");
		sb.append("\t unsubscribe to a key, supports specification using regular expression \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t disconnects from the server \n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t ");
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
