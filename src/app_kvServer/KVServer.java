package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.HashMap;

import logger.LogSetup;

import org.apache.log4j.*;

import client.KVCommInterface;
import client.KVStore;

import shared.messages.TextMessage;

public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate - in echoServer gave range 49152 to 65535
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	// private variables 
	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;

	private boolean running = false;

	private int port;
	private int cacheSize;
	private CacheStrategy cacheStrategy;


	/*     START OF DATA STRUCTURES FOR KEY VALUE STORAGE                */
	private HashMap<String, String> disk_storage;     //data structure for disk storage
	private HashMap<String, String> cache_LRU;     //data structure for cache_LRU case
	private HashMap<String, String> cache_LFU;     //data structure for cache_LFU case
	private HashMap<String, String> cache_FIFO;     //data structure for cache_FIFO case
	/*     END OF DATA STRUCTURES FOR KEY VALUE STORAGE                */


	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		disk_storage = new HashMap<String, String>(); //disk KVstorage
		if (strategy.equals("FIFO")) {
			this.cacheStrategy = CacheStrategy.FIFO;
		}
		else if (strategy.equals("LFU")) {
			this.cacheStrategy = CacheStrategy.LFU;
		}
		else if (strategy.equals("LRU")) {
			this.cacheStrategy = CacheStrategy.LRU;
		}
		else {
			this.cacheStrategy = CacheStrategy.FIFO; //in case of fail, just do FIFO operation
		}
        
	}
    
	
	@Override
	public int getPort(){
		return this.port;
	}

	@Override	
	public String getHostname(){
		InetAddress ip;
		String hostname;
		try {
			ip = InetAddress.getLocalHost();
			hostname = ip.getHostName();

			return hostname;
		} catch (UnknownHostException e) {
			e.printStackTrace();
	        
			logger.error("Error! " +
				"Unknown Host!. \n", e);
		
			return "Unknown Host";
		}
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return this.cacheStrategy;
	}

	@Override
	public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key){
        /*
            1) run incache
            2) then check if it is in cache
        */
        // Constraint checking for key and value
        /*
        if (key.getBytes("UTF-8").length > 20) {
            return false; //ERROR due to key length too long
        }
        */

		return false;
	}

	@Override
	public boolean inCache(String key){
        /*
            --> subpart of inStorage, check in 
        */
        // Constraint checking for key and value
        /*
        	if (key.getBytes("UTF-8").length > 20) {
            	return false; //ERROR due to key length too long
        }
        */

		return false;
	}

	@Override
	public String getKV(String key) throws Exception{        
        /*
            strategy:
            1) search cache first for key value pair: if not found
            2) search full list of memory for key value pair
        */
        // Constraint checking for key and value
		if (key.getBytes("UTF-8").length > 20) {
			return "ERROR"; //ERROR due to key length too long
		}

		return "test";
	}

	@Override
	public void putKV(String key, String value) throws Exception{
        /*
            strategy:
            1) use map to store data structure
        */
        // Constraint checking for key and value
		if (key.getBytes("UTF-8").length > 20) {
			return; //ERROR due to key length too long
		}

		//120kB is around 122880 bytes
		if (value.getBytes("UTF-8").length > 122880) {
			return; //ERROR due to value length too long
		}

		// if value is null, delete the key
		if (value == null) {
			if (disk_storage.containsKey(key)) {
				disk_storage.remove(key); //remove key if exists
				return;
			}
		}
		//else replace
		disk_storage.put(key,value);
	}

	@Override
	public void clearCache(){

	}

	@Override
	public void clearStorage(){

	}

	private boolean initializeServer() {
		logger.info("Initializing server...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
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
	public void run(){
		running = initializeServer();

		if(serverSocket != null) {
			while(this.running){
				try {
					Socket client = serverSocket.accept();                
					ClientConnection connection = 
							new ClientConnection(this, client);
					new Thread(connection).start();
	                
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
		//add actions to free data structures, cache, etc

		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

    /**
     * Main entry point for the server application. 
     * @param args contains the port number at args[0], cachesize at args[1], strategy at args[2].
     */
	public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <cache strategy>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String cacheStrategy = args[2];
				KVServer app = new KVServer(port, cacheSize, cacheStrategy);
				app.run();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument(s)!");
			System.out.println("Usage: Server <port> <cache size> <cache strategy>!");
			System.exit(1);
		}
    }
}
