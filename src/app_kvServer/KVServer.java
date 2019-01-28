package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.LinkedHashMap;

import logger.LogSetup;

import org.apache.log4j.*;

import client.KVCommInterface;
import client.KVStore;

import shared.messages.TextMessage;

public class KVServer implements IKVServer, Runnable {
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
//    static LinkedHashMap<String, String> cache_LRU;     //data structure for cache_LRU case
   // private LinkedHashMap<String, String> cache_FIFO;     //data structure for cache_FIFO case
    // private LFUCache lfucache;
    //private File outputFile;
    //private File tempFile;

    private String filename = "./persistantStorage.data";
    private String tempname = "./.temp.persistantStorage.data";
    private LinkedHashMap<String, String> cache_LRU = new LinkedHashMap<String, String>(getCacheSize(), .75f , true) { 
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) { 
            return size() > getCacheSize(); 
        } 
    }; 
    private LinkedHashMap <String, String> cache_FIFO = new LinkedHashMap<String, String>() { 
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) { 
            return size() > getCacheSize(); 
        } 
    }; 
    private LFUCache lfucache;
    private File outputFile = new File(filename);
    private File tempFile = new File(tempname);


    private BufferedWriter disk_write;
    private BufferedWriter temp_disk_write;
    private BufferedReader disk_read;
//    private LinkedHashMap<String, String> cache_LFU;     //data structure for cache_LFU case

    /*     END OF DATA STRUCTURES FOR KEY VALUE STORAGE                */

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
        this.port = port;
        this.cacheSize = cacheSize;
        //disk_storage = new HashMap<String, String>(); //disk KVstorage
 //       File outputFile = new File(filename);
 //       File tempFile = new File(tempname);

        try {
            outputFile.createNewFile();
            BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile,true)); //true so that any new data is just appended
            BufferedWriter temp_disk_write = new BufferedWriter(new FileWriter(tempFile,true)); //true so that any new data is just appended
        } catch (IOException ioe) {
            System.out.println("Trouble creating file: " + ioe.getMessage());
        }

        try {
            BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended
        } catch (FileNotFoundException e) {
            //create file anew if file is not found
            outputFile = new File(filename);
            try {
                outputFile.createNewFile();
            } catch (IOException ioe) {
                System.out.println("Trouble creating file: " + ioe.getMessage());
            }
        } catch (IOException ioe) {
            System.out.println("Trouble reading from the file: " + ioe.getMessage());
        }

        if (strategy.equals("FIFO")) {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO;
            System.out.println("Constructing cache_FIFO");
            /*
            LinkedHashMap <String, String> cache_FIFO = new LinkedHashMap<String, String>(getCacheSize()) { 
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) 
                    { 
                        return size() > getCacheSize(); 
                    } 
            }; 
            */

        }
        else if (strategy.equals("LFU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LFU;
            this.lfucache = new LFUCache(getCacheSize());
        }
        else if (strategy.equals("LRU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LRU;
            /*
            LinkedHashMap<String, String> cache_LRU =  
                new LinkedHashMap<String, String>(getCacheSize(), .75f , true) { 
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) 
                    { 
                        return size() > getCacheSize(); 
                    } 
            }; 
            */
        }
        else {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO; //in case of fail, just do FIFO operation
            /*
            LinkedHashMap <String, String> cache_FIFO = new LinkedHashMap<String, String>() { 
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) 
                    { 
                        return size() > getCacheSize(); 
                    } 
            }; 
            */
        }
        
	}

    public LinkedHashMap<String, String> getFIFO() {
        return this.cache_FIFO ;
    }

    public void putFIFO(String key, String value) {
        this.cache_FIFO.put(key,value);
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
        if (inCache(key)) {
            return true ;
        }

    */

        // iterate through memory to see if located in disk
        String strCurrentLine;
        try {
            BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile,true)); //true so that any new data is just appended
            BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended

            while ((strCurrentLine = disk_read.readLine()) != null) {
                String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                if (keyValue[0].equals(key)) {
                    return true;
                }
            }
            disk_read.close();
        } catch (IOException e) {
			System.out.println("Error! unable to read/write!");
		} 
        // no key found in cache nor storage

		return false;
	}


	@Override
    public boolean inCache(String key){

        if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
            // FIFO case
            return this.cache_FIFO.containsKey(key);
        } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
            // LRU case
            return cache_LRU.containsKey(key);
        } else {
            // LFU case
            return lfucache.lfu_containsKey(key);
        }
	}

	@Override
    public String getKV(String key) throws Exception{
        // Constraint checking for key and value
        if (key.getBytes("UTF-8").length > 20) {
            return "ERROR"; //ERROR due to key length too long
        }

        if (inCache(key)) {
            if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                // FIFO case
                return this.cache_FIFO.get(key);
            } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                // LRU case
                return cache_LRU.get(key);
            } else {
                // LFU case
                return lfucache.lfu_get(key);
            }
        }
        // iterate through memory to see if located in disk
        String strCurrentLine;
        try {
            BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended
        } catch (IOException ioe) {
            System.out.println("Trouble Reading file: " + ioe.getMessage());
            outputFile.createNewFile();
        }

        while ((strCurrentLine = disk_read.readLine()) != null) {
            String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
            if (keyValue[0].equals(key)) {
                return keyValue[1];
            }
        }
        disk_read.close();

		return "ERROR_NO_KEY_FOUND";
	}

	@Override
    public String putKV(String key, String value) throws Exception{
        /*
            strategy:
            1) use map to store data structure
        */
        // Constraint checking for key and value

		String result = "";		

        if (key.getBytes("UTF-8").length > 20) {
            return "ERROR"; //ERROR due to key length too long
        }

        //120kB is around 122880 bytes
        if (value.getBytes("UTF-8").length > 122880) {
            return "ERROR"; //ERROR due to value length too long
        }

        System.out.println("Entering");

        if (value == null) {
			result = "ERROR"; //error if deleting non-existent key

            // delete key
            if (inCache(key)) {
                if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                    // FIFO case
                    this.cache_FIFO.remove(key);
                } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                    // LRU case
                    cache_LRU.remove(key);
                } else {
                    // LFU case
                    lfucache.lfu_remove(key);
                }

				result = "UPDATE"; //delete successful
            }

            if (inStorage(key)){
                // need to remove the key from the list
                String strCurrentLine;
                try {
                    BufferedWriter temp_disk_write = new BufferedWriter(new FileWriter(tempFile,true)); //true so that any new data is just appended
                    BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended

                    while ((strCurrentLine = disk_read.readLine()) != null) {
                        String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                        if (!keyValue[0].equals(key)) {
                            temp_disk_write.write(strCurrentLine);
                        }
                    }
                    disk_read.close();
                    temp_disk_write.close();
                    // at end rename file
                    boolean success = tempFile.renameTo(outputFile); //renamed

					result = "UPDATE"; //delete successful
                } catch (IOException e) {
                    System.out.println("Error! unable to read!");
                } 

            }
        } else {
            // insert key in cache
            if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                // FIFO case
                System.out.println("Inserting FIFO Value");
                if (this.cache_FIFO == null) {
                    System.out.println("ERROR: cache_FIFO is null");
                }
				if (this.cache_FIFO.containsKey(key)) {
					result = "UPDATE";
				} else {
					result = "SUCCESS";
				}
                this.cache_FIFO.put(key,value);
                System.out.println("After FIFO Value");
                // print stuff out
                for (Map.Entry<String, String> entry : cache_FIFO.entrySet()) {
                    String mapvalue = entry.getValue();
                    String mapkey = entry.getKey();
                    //Do something
                    System.out.println("System key: " + mapkey + " with value: " + mapvalue);
                 }

            } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                // LRU case
				if (cache_LRU.containsKey(key)) {
					result = "UPDATE";
				} else {
					result = "SUCCESS";
				}
                cache_LRU.put(key,value);
            } else {
                // LFU case
				if (lfucache.lfu_containsKey(key)) {
					result = "UPDATE";
				} else {
					result = "SUCCESS";
				}
                System.out.println("Before LFU Value");
                lfucache.lfu_put(key,value);
            }

            // insert key in storage
            if (!inStorage(key)) {
                BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile,true)); //true so that any new data is just appended
                String diskEntry = key + " " + value + "\n" ;
                disk_write.write(diskEntry);
                disk_write.close();
            }

        }

		return result;


	}


	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
        if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
            // FIFO case
            cache_FIFO.clear();
        } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
            // LRU case
            cache_LRU.clear();
        } else {
            // LFU case
            lfucache.lfu_clear();
        }

	}

	@Override
    public void clearStorage(){
		// TODO Auto-generate method stub
            clearCache();        //clear the cache
            //delete memory file on disk
            try {
                outputFile.delete();
                outputFile.createNewFile();
            } catch (IOException ioe) {
                System.out.println("Trouble deleting/creating file: " + ioe.getMessage());
            }
	}

	private boolean initializeServer() {
		logger.info("Initializing server...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			logger.info("Server cache strategy type is " 
					+ cacheStrategy + " with size "
					+ cacheSize);    
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
