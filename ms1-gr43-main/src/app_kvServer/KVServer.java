package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.ArrayList;
import logger.LogSetup;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.ClientConnection;
import app_kvServer.cache.CacheInterface;
import app_kvServer.cache.FIFO;
import app_kvServer.cache.LFU;
import app_kvServer.cache.LRU;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.*;
import java.net.InetAddress;
// import java.util.ArrayList;
// import java.util.List;

public class KVServer implements IKVServer {
	private static Logger logger = Logger.getRootLogger();
	private int port;
	private int cacheSize;
	private String strategy;

	private ServerSocket serverSocket;
    private boolean running;
	private FileDB fileDb;
	// private FIFO cache;
	private CacheInterface cache = null;
	private ArrayList<Thread> connections;
	
	private static String dataPath;

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
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
		running = initializeServer();
		connections = new ArrayList<Thread>();
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
		//running = initializeServer();
		//connections = new ArrayList<Thread>();
        
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this);
	                
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

	
	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());  
					
			fileDb = new FileDB(dataPath);
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
		// TODO Auto-generated method stub
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

	public static void main(String[] args) {
    	try {
			// if(args.length != 3) {
			// 	System.out.println("Error! Invalid number of arguments!");
			// 	System.out.println("Usage: Server <port>!");
			// } else {

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

				Option strategyOp = new Option("st", "strategy", true, "Cache Strategy");
				strategyOp.setRequired(false);
				options.addOption(strategyOp);

				Option cacheOp = new Option("cs", "cacheSize", true, "Cache Size");
				cacheOp.setRequired(false);
				options.addOption(cacheOp);

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
					address = InetAddress.getLocalHost().toString();
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
				if (!cmd.hasOption("st")) {
					strategy = "FIFO";
				} else{
					strategy = cmd.getOptionValue("strategy");
				}

				int cacheSize = 4096;
				if (!cmd.hasOption("cs")) {
					cacheSize = 4096;
				} else{
					try{
						cacheSize = Integer.parseInt(cmd.getOptionValue("cacheSize"));
					}catch(NumberFormatException ex){
						ex.printStackTrace();
					}
				}
				// logLevel = Level.ALL;
				new LogSetup(logPath, logLevel);
				logger.info("cache strategy is: " + strategy);

				// int cacheSize = Integer.parseInt(args[1]);
				// String strategy = args[2];
				// int cacheSize = 4096;
				// String strategy = "FIFO";

				new KVServer(port, cacheSize, strategy).run();
			// }
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
