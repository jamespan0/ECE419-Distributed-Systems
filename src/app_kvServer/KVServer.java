package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.HashMap;

//import logging.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

    // private variables for getters 
    private int port;
    private int cacheSize;
    private IKVServer.CacheStrategy cacheStrategy;


    /*     START OF DATA STRUCTURES FOR KEY VALUE STORAGE                */
    private HashMap<String, String> disk_storage;     //data structure for disk storage
    private HashMap<String, String> cache_LRU;     //data structure for cache_LRU case
    private HashMap<String, String> cache_LFU;     //data structure for cache_LFU case
    private HashMap<String, String> cache_FIFO;     //data structure for cache_FIFO case
    /*     END OF DATA STRUCTURES FOR KEY VALUE STORAGE                */


	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
        this.port = port;
        this.cacheSize = cacheSize;
        disk_storage = new HashMap<String, String>(); //disk KVstorage
        if (strategy.equals("FIFO")) {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO;
        }
        else if (strategy.equals("LFU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LFU;
        }
        else if (strategy.equals("LRU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LRU;
        }
        else {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO; //in case of fail, just do FIFO operation
        }
        
	}
    
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return this.cacheStrategy;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
        
        /*
            strategy:
            1) search cache first for key value pair: if not found
            2) search full list of memory for key value pair
        */
        // Constraint checking for key and value
        if (key.getBytes("UTF-8").length > 20) {
            return "ERROR"; //ERROR due to key length too long
        }


		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}
}
