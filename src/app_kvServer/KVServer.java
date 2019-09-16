package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.CountDownLatch;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import ecs.IECSNode;
import ecs.ECSNode;

import logger.LogSetup;

import org.apache.log4j.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;

import org.json.*;

import client.KVCommInterface;
import client.KVStore;

import shared.messages.KVAdminMessage;
import shared.messages.TextMessage;
import shared.messages.ClientSocketListener;

public class KVServer implements IKVServer, Runnable, Watcher, ClientSocketListener {
    /**
     * Start KV Server at given port
     *
     * @param port given port for storage server to operate - in echoServer gave range 49152 to 65535
     * @param cacheSize specifies how many key-value pairs the server is allowed
     * to keep in-memory
     * @param strategy specifies the cache replacement strategy in case the cache
     * is full and there is a GET- or PUT-request on a key that is
     * currently not contained in the cache. Options are "FIFO", "LRU",
     * and "LFU".
     */

    // private variables
    private static Logger logger = Logger.getRootLogger();
    private ServerSocket serverSocket;
    private int port;
    private int cacheSize;
    private CacheStrategy cacheStrategy;

    /*     START OF DATA STRUCTURES FOR KEY VALUE STORAGE                */
    private String filename;
    private String tempname;
    private LinkedHashMap<String, String> cache_LRU = new LinkedHashMap<String, String>(getCacheSize(), .75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > getCacheSize();
        }
    };
    private LinkedHashMap<String, String> cache_FIFO = new LinkedHashMap<String, String>() {
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
            return size() > getCacheSize();
        }
    };
    private LFUCache lfucache;
    private File outputFile;
    private File tempFile;

    private BufferedWriter disk_write;
    private BufferedWriter temp_disk_write;
    private BufferedReader disk_read;


    /*     END OF DATA STRUCTURES FOR KEY VALUE STORAGE                */



    /*  M2 variables start  */

    private int m2_port;
    private int m2_cachesize;
    private StringBuffer stringBuffer;  //hash of tuple encrypted

    private boolean running = false;
    public boolean activated = false;
    public boolean writeLock = false;
    public boolean transferConnected = false;
    private boolean serverInitSuccess = false;
    private boolean moveComplete = false;
//    private serverTypes serverStatus;

    private String serverName;

    private static final String ZK_CONNECT = "127.0.0.1:2181";
    private static final int ZK_TIMEOUT = 2000;

    private ZooKeeper zk;
    private CountDownLatch connectedSignal;

    private String rangeLow;
    private String rangeHigh;
    public Map<String, ECSNode> metaData = new HashMap<String, ECSNode>();
    public JSONObject JSONmetaData;
    public Set<String> replicas = new TreeSet<String>();
    private Map<String, String[]> replicaOf = new HashMap<String, String[]>();

    private Set<String> keyTable = new TreeSet<String>();
    private int heartbeatNum = 0;


/*
    public enum serverTypes {
        SERVER_WRITE_LOCK,    //do not process ECS nor client requests, server shut down
        SERVER_STOPPED, //block all client requests, only process ECS
        SERVER_    // all client and ECS requests are processed
    }
*/


    /*  M2 variables end */

    //M1 KVServer
    public KVServer(int port, String serverName) {
        this.serverName = serverName;

        filename = "./" + serverName + "PersistantStorage.data";
        tempname = "./.temp." + serverName + "PersistantStorage.data";

        outputFile = new File(filename);
        tempFile = new File(tempname);

        this.port = port;

        try {
            outputFile.createNewFile();
            BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));

            String line;

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(" ");

                keyTable.add(tokens[0]);

            }

            connectedSignal = new CountDownLatch(1);

            zk = new ZooKeeper(ZK_CONNECT, ZK_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });

            connectedSignal.await();

            logger.info("New ZooKeeper connection at: " + ZK_CONNECT);

            zk.register(this);

            Stat st = zk.exists("/" + serverName, true);

            if (st == null) {
                zk.create("/" + serverName, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                st = zk.exists("/" + serverName + "/message", false);
                zk.setData("/" + serverName + "/message", "".getBytes(), st.getVersion());
                st = zk.exists("/" + serverName + "/message", true);
            }

            st = zk.exists("/activeNodes/" + serverName, false);

            if (st == null) {
                zk.create("/activeNodes/" + serverName, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            logger.info("Znode /" + serverName + " initialized");

        } catch (KeeperException | IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }


    }

    //metadata is string
    //cacheSize is int
    //replacementstrategy is String
    public void initKVServer(String _metaData, int cacheSize, String strategy) {


        this.m2_cachesize = cacheSize;

        activated = false;
        writeLock = false;

        this.cacheSize = cacheSize;

        try {
            outputFile.createNewFile();
            BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile, true));
            BufferedWriter temp_disk_write = new BufferedWriter(new FileWriter(tempFile, true));
        } catch (IOException ioe) {
            System.out.println("Trouble creating file: " + ioe.getMessage());
        }

        try {
            BufferedReader disk_read = new BufferedReader(new FileReader(outputFile));
        } catch (FileNotFoundException e) {
            //create file anew if file is not found
            outputFile = new File(filename);
            try {
                outputFile.createNewFile();
            } catch (IOException ioe) {
                System.out.println("Trouble creating file: " + ioe.getMessage());
            }
        }

        if (strategy.equals("FIFO")) {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO;
            System.out.println("Constructing FIFO cache");

        } else if (strategy.equals("LFU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LFU;
            this.lfucache = new LFUCache(getCacheSize());
            System.out.println("Constructing LFU cache");
        } else if (strategy.equals("LRU")) {
            this.cacheStrategy = IKVServer.CacheStrategy.LRU;
            System.out.println("Constructing LRU cache");
        } else {
            this.cacheStrategy = IKVServer.CacheStrategy.FIFO; //in case of fail, just do FIFO operation
            System.out.println("Constructing FIFO cache");
        }

        JSONmetaData = new JSONObject(_metaData);

        JSONObject thisNode = JSONmetaData.getJSONObject(serverName);

        this.rangeLow = thisNode.getString("rangeLow");
        this.rangeHigh = thisNode.getString("rangeHigh");

        Set<String> keySet = JSONmetaData.keySet();

        String replicaOf1 = "";
        String replicaOf2 = "";

        for (String key : keySet) {
            if (!key.equals("transfer")) {
                JSONObject node = JSONmetaData.getJSONObject(key);
                String range[] = {node.getString("rangeLow"), node.getString("rangeHigh")};
                String host = node.getString("host");
                int port = node.getInt("port");

                metaData.put(key, new ECSNode(key, host, port, range));

                if (key.equals(serverName)) {
                    String temp1 = node.getJSONArray("replicas").getString(0);
                    String temp2 = node.getJSONArray("replicas").getString(1);

                    if (!temp1.equals("")) {
                        if (!replicas.contains(temp1)) {
                            logger.info("NEW REPLICA: " + serverName + "->" + temp1);
                        }
                    }


                    if (!temp2.equals("")) {
                        if (!replicas.contains(temp2)) {
                            logger.info("NEW REPLICA: " + serverName + "->" + temp2);
                        }
                    }

                    replicas.clear();
                    replicas.add(temp1);
                    replicas.add(temp2);

                    replicaOf1 = node.getJSONArray("replicaOf").getString(0);
                    replicaOf2 = node.getJSONArray("replicaOf").getString(1);
                }
            }
        }

        replicaOf.clear();
        if (!replicaOf1.equals("")) {
            String[] replicaRange1 = {metaData.get(replicaOf1).getNodeHashRange()[0], metaData.get(replicaOf1).getNodeHashRange()[1]};
            replicaOf.put(replicaOf1, replicaRange1);
        }

        if (!replicaOf2.equals("")) {
            String[] replicaRange2 = {metaData.get(replicaOf2).getNodeHashRange()[0], metaData.get(replicaOf2).getNodeHashRange()[1]};
            replicaOf.put(replicaOf2, replicaRange2);
        }

        try {
            zk.setData("/ecsMessages", (serverName + " initialization success!").getBytes(),
                    zk.exists("/ecsMessages", false).getVersion());

            logger.info("Server initialized with cacheSize = " + cacheSize + ", cacheStrategy = " + this.cacheStrategy);
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public void start() {
        activated = true;
        logger.info("Server activated on port: "
                + serverSocket.getLocalPort());

        try {
            zk.setData("/ecsMessages", (serverName + " Started").getBytes(),
                    zk.exists("/ecsMessages", false).getVersion());
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void stop() {
        try {
            activated = true;

            zk.setData("/ecsMessages", (serverName + " Stopped").getBytes(),
                    zk.exists("/ecsMessages", false).getVersion());

            logger.info("Server deactivated on port: "
                    + serverSocket.getLocalPort());
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void shutDown(String shutdownMessage) {
        this.lockWrite();
        String[] tokens = shutdownMessage.split(" ");

        if (tokens.length > 1) {
            String host = tokens[1];
            int port = Integer.parseInt(tokens[2]);

            try {
                int timeout = 5000;
                long currentTime = System.currentTimeMillis();

                while (System.currentTimeMillis() - currentTime < timeout) {
                }

                KVStore client = new KVStore(host, port);
                client.connect();
                client.addListener(this);
                client.start();
                String message = "MOVE_DATA ";

                JSONArray data = new JSONArray();
                List<String> deleteKeys = new ArrayList<String>();

                for (String temp : keyTable) {
                    JSONObject entry = new JSONObject();
                    entry.put("key", temp);
                    entry.put("value", getKV(temp));

                    data.put(entry);
                    deleteKeys.add(temp);
                }


                TextMessage msg = new TextMessage(message + data.toString());

                currentTime = System.currentTimeMillis();

                while (System.currentTimeMillis() - currentTime < timeout && !transferConnected) {

                }

                transferConnected = false;

                client.sendMessage(msg);

                zk.setData("/ecsMessages", ("TRANSFER_SUCCESS").getBytes(),
                        zk.exists("/ecsMessages", false).getVersion());

                for (String temp : deleteKeys) {
                    synchronized (this) {
                        putKV(temp, "null");
                    }
                }

                deleteKeys.clear();

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }

        try {
            activated = true;

            zk.setData("/ecsMessages", (serverName + " shutdown!").getBytes(),
                    zk.exists("/ecsMessages", false).getVersion());
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        clearCache();
        close();
    }

    public void lockWrite() {
        writeLock = true;

        logger.info("Server write lock enabled");
    }

    public void unLockWrite() {
        writeLock = false;

        logger.info("Server write lock disabled");
    }

    @Override
    public void handleNewMessage(TextMessage msg) {
        String[] tokens = msg.getMsg().split(" ");

        if (tokens[0].equals("CONNECTION_SUCCESS")) {
            transferConnected = true;
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
    }

    //range should ideally be in head
    public void moveData(String prevRangeLow, String rangeHigh, String name, String host, int port) {
        this.lockWrite();

        try {
            int timeout = 5000;
            long currentTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - currentTime < timeout) {
            }

            KVStore client = new KVStore(host, port);
            client.connect();
            client.addListener(this);
            client.start();
            String message = "MOVE_DATA ";

            JSONArray data = new JSONArray();

            String rangeLowTemp = String.format("%-32s", prevRangeLow).replace(' ', prevRangeLow.charAt(prevRangeLow.length() - 1));
            String rangeHighTemp = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));
            BigInteger rangeLow_ = new BigInteger(rangeLowTemp, 16);
            BigInteger rangeHigh_ = new BigInteger(rangeHighTemp, 16);

            List<String> deleteKeys = new ArrayList<String>();

            for (String temp : keyTable) {
                BigInteger key = new BigInteger(temp, 16);

                if (key.compareTo(rangeLow_) != -1 && key.compareTo(rangeHigh_) != 1) {
                    JSONObject entry = new JSONObject();
                    entry.put("key", temp);
                    entry.put("value", getKV(temp));

                    data.put(entry);

                    deleteKeys.add(temp);
                }
            }

            TextMessage msg = new TextMessage(message + data.toString());

            currentTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - currentTime < timeout && !transferConnected) {

            }

            transferConnected = false;

            client.sendMessage(msg);

            moveComplete = true;

            zk.setData("/ecsMessages", ("TRANSFER_SUCCESS").getBytes(),
                    zk.exists("/ecsMessages", false).getVersion());

            if (!replicaOf.containsKey(name)) {
                for (String temp : deleteKeys) {
                    synchronized (this) {
                        putKV(temp, "null");
                    }
                }
            }

            deleteKeys.clear();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }

        this.unLockWrite();
    }

    public void update(String _metaData) {
        JSONmetaData = new JSONObject(_metaData);

        JSONObject thisNode = JSONmetaData.getJSONObject(serverName);
        String prevRangeLow = this.rangeLow;
        this.rangeLow = thisNode.getString("rangeLow");
        this.rangeHigh = thisNode.getString("rangeHigh");

        String rangeHighTransfer = Integer.toHexString(Integer.parseInt(rangeLow, 16) - 1);
        String transferHost = "";
        int transferPort = 0;
        String transferName = "";

        Set<String> keySet = JSONmetaData.keySet();

        metaData.clear();

        String replicaOf1 = "";
        String replicaOf2 = "";

        for (String key : keySet) {
            if (!key.equals("transfer")) {
                JSONObject node = JSONmetaData.getJSONObject(key);
                String range[] = {node.getString("rangeLow"), node.getString("rangeHigh")};
                String host = node.getString("host");
                int port = node.getInt("port");

                if (range[1].equals(rangeHighTransfer)) {
                    transferHost = host;
                    transferPort = port;
                    transferName = key;
                }

                metaData.put(key, new ECSNode(key, host, port, range));

                if (key.equals(serverName)) {
                    String temp1 = node.getJSONArray("replicas").getString(0);
                    String temp2 = node.getJSONArray("replicas").getString(1);

                    if (!temp1.equals("")) {
                        if (!replicas.contains(temp1)) {
                            logger.info(temp1 + " is now replica of " + serverName);
                        }
                    }

                    if (!temp2.equals("")) {
                        if (!replicas.contains(temp2)) {
                            logger.info(temp2 + " is now replica of " + serverName);
                        }
                    }

                    replicas.clear();
                    replicas.add(temp1);
                    replicas.add(temp2);

                    replicaOf1 = node.getJSONArray("replicaOf").getString(0);
                    replicaOf2 = node.getJSONArray("replicaOf").getString(1);
                }
            }

        }

        replicaOf.clear();
        if (!replicaOf1.equals("")) {
            String[] replicaRange1 = {metaData.get(replicaOf1).getNodeHashRange()[0], metaData.get(replicaOf1).getNodeHashRange()[1]};
            replicaOf.put(replicaOf1, replicaRange1);
        }

        if (!replicaOf2.equals("")) {
            String[] replicaRange2 = {metaData.get(replicaOf2).getNodeHashRange()[0], metaData.get(replicaOf2).getNodeHashRange()[1]};
            replicaOf.put(replicaOf2, replicaRange2);
        }

        if (JSONmetaData.getBoolean("transfer")) {
            moveData(prevRangeLow, rangeHighTransfer, transferName, transferHost, transferPort);
        }

        try {
            int timeout = 5000;
            long currentTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - currentTime < timeout && !moveComplete) {
            }
            moveComplete = false;


            List<String> deleteKeys = new ArrayList<String>();

            for (String temp : keyTable) {
                boolean valid = false;

                BigInteger keyTemp = new BigInteger(temp, 16);
                String rangeLowTemp = String.format("%-32s", rangeLow).replace(' ', rangeLow.charAt(rangeLow.length() - 1));
                String rangeHighTemp = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));

                BigInteger replicaLow = new BigInteger(rangeLowTemp, 16);
                BigInteger replicaHigh = new BigInteger(rangeHighTemp, 16);


                if (keyTemp.compareTo(replicaLow) != -1 && keyTemp.compareTo(replicaHigh) != 1) {
                    valid = true;
                }
                logger.info("\n CHECKING RANGES - " + temp);
                logger.info(rangeLow + " -> " + rangeHigh + " = " + valid);

                for (String replicaOfTemp : replicaOf.keySet()) {
                    String replicaRange[] = replicaOf.get(replicaOfTemp);
                    rangeLowTemp = String.format("%-32s", replicaRange[0]).replace(' ', replicaRange[0].charAt(replicaRange[0].length() - 1));
                    rangeHighTemp = String.format("%-32s", replicaRange[1]).replace(' ', replicaRange[1].charAt(replicaRange[1].length() - 1));
                    replicaLow = new BigInteger(rangeLowTemp, 16);
                    replicaHigh = new BigInteger(rangeHighTemp, 16);

                    if (keyTemp.compareTo(replicaLow) != -1 && keyTemp.compareTo(replicaHigh) != 1) {
                        valid = true;
                    }

                    logger.info(rangeLowTemp + " -> " + rangeLowTemp + " = " + valid);
                }

                if (!valid) {
                    deleteKeys.add(temp);
                }
            }

            for (String deleteTemp : deleteKeys) {
                synchronized (this) {
                    putKVNoReplicate(deleteTemp, "null");
                }
            }


            String low = String.format("%-32s", rangeLow).replace(' ', rangeLow.charAt(rangeLow.length() - 1));
            String high = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));

            BigInteger rangeLowInt = new BigInteger(low, 16);
            BigInteger rangeHighInt = new BigInteger(high, 16);

            for (String replica : replicas) {
                if (!replica.equals("")) {
                    JSONArray data = new JSONArray();

                    for (String key : keyTable) {

                        BigInteger keyInt = new BigInteger(key, 16);

                        if (keyInt.compareTo(rangeLowInt) != -1 && keyInt.compareTo(rangeHighInt) != 1) {
                            JSONObject entry = new JSONObject();
                            entry.put("key", key);
                            entry.put("value", getKV(key));

                            data.put(entry);
                        }
                    }

                    currentTime = System.currentTimeMillis();

                    while (System.currentTimeMillis() - currentTime < timeout) {
                    }

                    KVStore client = new KVStore(metaData.get(replica).getNodeHost(), metaData.get(replica).getNodePort());
                    client.connect();
                    client.addListener(this);
                    client.start();

                    TextMessage msg = new TextMessage("REPLICATE " + data.toString());

                    currentTime = System.currentTimeMillis();

                    while (System.currentTimeMillis() - currentTime < timeout && !transferConnected) {

                    }

                    transferConnected = false;

                    client.sendMessage(msg);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            e.printStackTrace();
        }

    }

    public LinkedHashMap<String, String> getFIFO() {
        return this.cache_FIFO;
    }

    public void putFIFO(String key, String value) {
        this.cache_FIFO.put(key, value);
    }


    @Override
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public String getHostname() {
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
    public CacheStrategy getCacheStrategy() {
        return this.cacheStrategy;
    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    // returns hash of String
    public String hashing(String message) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(message.getBytes());
            byte[] messageDigestMD5 = messageDigest.digest();
            StringBuffer stringBuffer = new StringBuffer();
            for (byte bytes : messageDigestMD5) {
                stringBuffer.append(String.format("%02x", bytes & 0xff));
            }

            return stringBuffer.toString();

        } catch (NoSuchAlgorithmException exception) {
            exception.printStackTrace();
        }
        return "";
    }

    public boolean checkResponsibility(String key) {

        String low = String.format("%-32s", rangeLow).replace(' ', rangeLow.charAt(rangeLow.length() - 1));
        String high = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));

        BigInteger lowBigInt = new BigInteger(low, 16);
        BigInteger highBigInt = new BigInteger(high, 16);
        BigInteger keyBigInt = new BigInteger(key, 16);

        if (keyBigInt.compareTo(lowBigInt) == -1 || keyBigInt.compareTo(lowBigInt) == -1) {
            return false;
        }

        return true;
    }

    public boolean checkGetResposibility(String key) {
        boolean valid = false;

        BigInteger keyTemp = new BigInteger(key, 16);
        String rangeLowTemp = String.format("%-32s", rangeLow).replace(' ', rangeLow.charAt(rangeLow.length() - 1));
        String rangeHighTemp = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));

        BigInteger replicaLow = new BigInteger(rangeLowTemp, 16);
        BigInteger replicaHigh = new BigInteger(rangeHighTemp, 16);


        if (keyTemp.compareTo(replicaLow) != -1 && keyTemp.compareTo(replicaHigh) != 1) {
            valid = true;
        }
        logger.info("\n CHECKING RANGES - " + key);
        logger.info(rangeLow + " -> " + rangeHigh + " = " + valid);

        for (String replicaOfTemp : replicaOf.keySet()) {
            String replicaRange[] = replicaOf.get(replicaOfTemp);
            rangeLowTemp = String.format("%-32s", replicaRange[0]).replace(' ', replicaRange[0].charAt(replicaRange[0].length() - 1));
            rangeHighTemp = String.format("%-32s", replicaRange[1]).replace(' ', replicaRange[1].charAt(replicaRange[1].length() - 1));
            replicaLow = new BigInteger(rangeLowTemp, 16);
            replicaHigh = new BigInteger(rangeHighTemp, 16);

            if (keyTemp.compareTo(replicaLow) != -1 && keyTemp.compareTo(replicaHigh) != 1) {
                valid = true;
            }

            logger.info(rangeLowTemp + " -> " + rangeLowTemp + " = " + valid);
        }

        return valid;

    }

    @Override
    public boolean inStorage(String key) {

        // iterate through memory to see if located in disk
        String strCurrentLine;
        try {
            BufferedReader disk_read = new BufferedReader(new FileReader(outputFile));

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
    public boolean inCache(String key) {

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
    public String getKV(String key) throws Exception {

        // Constraint checking for key and value
        if (key.contains(" ") || key == "") {
            return "ERROR"; //ERROR due to whitespace
        }


        if (inCache(key)) {
            if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                // FIFO case
                String value = cache_FIFO.get(key);
                return value;
            } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                // LRU case
                String value = cache_LRU.get(key);
                return value;
            } else {
                // LFU case
                String value = lfucache.lfu_get(key);
                return value;
            }
        }
        // iterate through memory to see if located in disk
        String strCurrentLine;
        try {
            disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended
            while ((strCurrentLine = disk_read.readLine()) != null) {
                String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                if (keyValue[0].equals(key)) {
                    return keyValue[1];
                }
            }
            disk_read.close();
        } catch (IOException ioe) {
            System.out.println("Trouble Reading file: " + ioe.getMessage());
            outputFile.createNewFile();
        }

        return "ERROR_NO_KEY_FOUND";
    }

    @Override
    public String putKV(String key, String value) throws Exception {
        /*
            strategy:
            1) use map to store data structure
        */

        // Constraint checking for key and value
        if (key.contains(" ") || key == "") {
            return "ERROR"; //ERROR due to whitespace
        }

        String result = "";

        if (value.equals("null")) {
            result = "ERROR"; //error if deleting non-existent key
            System.out.println("TEST GOES INTO VALUE EQUALS NULL");

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

                result = "DELETE"; //delete successful
            }

            if (inStorage(key)) {
                // need to remove the key from the list
                String strCurrentLine;
                try {
                    BufferedWriter temp_disk_write = new BufferedWriter(new FileWriter(tempFile, true)); //true so that any new data is just appended
                    BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended

                    while ((strCurrentLine = disk_read.readLine()) != null) {
                        String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                        if (!keyValue[0].equals(key)) {
                            temp_disk_write.write(strCurrentLine + "\n");
                        }
                    }
                    disk_read.close();
                    temp_disk_write.close();
                    // at end rename file
                    boolean success = tempFile.renameTo(outputFile); //renamed

                    result = "DELETE"; //delete successful
                } catch (IOException e) {
                    System.out.println("Error! unable to read!");
                }

            }

            keyTable.remove(key);

        } else {

            // insert key in cache
            if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                // FIFO case
                if (this.cache_FIFO == null) {
                    System.out.println("ERROR: cache_FIFO is null");
                }
                if (this.cache_FIFO.containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                this.cache_FIFO.put(key, value);
                // print stuff out

            } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                // LRU case
                if (cache_LRU.containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                cache_LRU.put(key, value);
            } else {
                // LFU case
                if (lfucache.lfu_containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                lfucache.lfu_put(key, value);
            }

            // insert key in storage


            if (inStorage(key)) {
                // if key already in storage check value
                String strCurrentLine;
                boolean change = false;

                try {
                    BufferedReader disk_read = new BufferedReader(new FileReader(outputFile));

                    while ((strCurrentLine = disk_read.readLine()) != null) {
                        String[] keyValueA = strCurrentLine.split(" "); // keyValue[0] is the key
                        if (keyValueA[0].equals(key)) {
                            if (!keyValueA[1].equals(value)) {
                                change = true;
                                System.out.println("CHANGE GOES TO TRUE");
                                //remove key from disk
                            }
                        }
                    }
                    disk_read.close();
                } catch (IOException e) {
                    System.out.println("Error! unable to read/write!");
                }

                if (change) {
                    try {
                        BufferedWriter temp_disk_write1 = new BufferedWriter(new FileWriter(tempFile, true));
                        BufferedReader disk_read1 = new BufferedReader(new FileReader(outputFile));

                        while ((strCurrentLine = disk_read1.readLine()) != null) {
                            String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                            if (!keyValue[0].equals(key)) {
                                temp_disk_write1.write(strCurrentLine + "\n");
                            }
                        }
                        disk_read1.close();
                        temp_disk_write1.close();
                        // at end rename file
                        boolean success = tempFile.renameTo(outputFile); //renamed

//                                    result = "UPDATE"; //delete successful
                    } catch (IOException e) {
                        System.out.println("Error! unable to read!");
                    }

                    BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile, true)); //true so that any new data is just appended
                    String diskEntry = key + " " + value + "\n";
                    disk_write.write(diskEntry);
                    disk_write.close();
                }

            } else {
                BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile, true)); //true so that any new data is just appended
                String diskEntry = key + " " + value + "\n";
                disk_write.write(diskEntry);
                disk_write.close();

            }

            keyTable.add(key);


        }

        if (result.equals("SUCCESS") || result.equals("UPDATE")) {
            try {

                String low = String.format("%-32s", rangeLow).replace(' ', rangeLow.charAt(rangeLow.length() - 1));
                String high = String.format("%-32s", rangeHigh).replace(' ', rangeHigh.charAt(rangeHigh.length() - 1));

                BigInteger rangeLowInt = new BigInteger(low, 16);
                BigInteger rangeHighInt = new BigInteger(high, 16);

                JSONArray data = new JSONArray();

                for (String replica : replicas) {
                    if (!replica.equals("")) {
                        JSONObject entry = new JSONObject();
                        entry.put("key", key);
                        entry.put("value", value);

                        data.put(entry);

                        KVStore client = new KVStore(metaData.get(replica).getNodeHost(), metaData.get(replica).getNodePort());
                        client.connect();
                        client.addListener(this);
                        client.start();

                        TextMessage msg = new TextMessage("REPLICATE " + data.toString());

                        int timeout = 5000;
                        long currentTime = System.currentTimeMillis();

                        while (System.currentTimeMillis() - currentTime < timeout && !transferConnected) {

                        }

                        transferConnected = false;

                        client.sendMessage(msg);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                e.printStackTrace();
            }
        }

        return result;
    }

    public String putKVNoReplicate(String key, String value) throws Exception {
        /*
            strategy:
            1) use map to store data structure
        */

        // Constraint checking for key and value
        if (key.contains(" ") || key == "") {
            return "ERROR"; //ERROR due to whitespace
        }

        String result = "";

        if (value.equals("null")) {
            result = "ERROR"; //error if deleting non-existent key
            System.out.println("TEST GOES INTO VALUE EQUALS NULL");

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

            if (inStorage(key)) {
                // need to remove the key from the list
                String strCurrentLine;
                try {
                    BufferedWriter temp_disk_write = new BufferedWriter(new FileWriter(tempFile, true)); //true so that any new data is just appended
                    BufferedReader disk_read = new BufferedReader(new FileReader(outputFile)); //true so that any new data is just appended

                    while ((strCurrentLine = disk_read.readLine()) != null) {
                        String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                        if (!keyValue[0].equals(key)) {
                            temp_disk_write.write(strCurrentLine + "\n");
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

            keyTable.remove(key);

        } else {

            // insert key in cache
            if (getCacheStrategy() == IKVServer.CacheStrategy.FIFO) {
                // FIFO case
                if (this.cache_FIFO == null) {
                    System.out.println("ERROR: cache_FIFO is null");
                }
                if (this.cache_FIFO.containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                this.cache_FIFO.put(key, value);
                // print stuff out

            } else if (getCacheStrategy() == IKVServer.CacheStrategy.LRU) {
                // LRU case
                if (cache_LRU.containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                cache_LRU.put(key, value);
            } else {
                // LFU case
                if (lfucache.lfu_containsKey(key)) {
                    result = "UPDATE";
                } else {
                    result = "SUCCESS";
                }
                lfucache.lfu_put(key, value);
            }

            // insert key in storage


            if (inStorage(key)) {
                // if key already in storage check value
                String strCurrentLine;
                boolean change = false;

                try {
                    BufferedReader disk_read = new BufferedReader(new FileReader(outputFile));

                    while ((strCurrentLine = disk_read.readLine()) != null) {
                        String[] keyValueA = strCurrentLine.split(" "); // keyValue[0] is the key
                        if (keyValueA[0].equals(key)) {
                            if (!keyValueA[1].equals(value)) {
                                change = true;
                                System.out.println("CHANGE GOES TO TRUE");
                                //remove key from disk
                            }
                        }
                    }
                    disk_read.close();
                } catch (IOException e) {
                    System.out.println("Error! unable to read/write!");
                }

                if (change) {
                    try {
                        BufferedWriter temp_disk_write1 = new BufferedWriter(new FileWriter(tempFile, true));
                        BufferedReader disk_read1 = new BufferedReader(new FileReader(outputFile));

                        while ((strCurrentLine = disk_read1.readLine()) != null) {
                            String[] keyValue = strCurrentLine.split(" "); // keyValue[0] is the key
                            if (!keyValue[0].equals(key)) {
                                temp_disk_write1.write(strCurrentLine + "\n");
                            }
                        }
                        disk_read1.close();
                        temp_disk_write1.close();
                        // at end rename file
                        boolean success = tempFile.renameTo(outputFile); //renamed

//                                    result = "UPDATE"; //delete successful
                    } catch (IOException e) {
                        System.out.println("Error! unable to read!");
                    }

                    BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile, true)); //true so that any new data is just appended
                    String diskEntry = key + " " + value + "\n";
                    disk_write.write(diskEntry);
                    disk_write.close();
                }

            } else {
                BufferedWriter disk_write = new BufferedWriter(new FileWriter(outputFile, true)); //true so that any new data is just appended
                String diskEntry = key + " " + value + "\n";
                disk_write.write(diskEntry);
                disk_write.close();

            }

            keyTable.add(key);


        }

        return result;


    }

    @Override
    public void clearCache() {
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
    public void clearStorage() {
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

            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            List<String> children = zk.getChildren("/" + serverName, false);

            if (children.isEmpty()) {
                zk.exists("/" + serverName, true);
                zk.create("/" + serverName + "/message", "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.exists("/" + serverName + "/message", true);
                return;
            }

            KVAdminMessage message = new KVAdminMessage(zk.getData("/" + serverName + "/message", true, null));

            logger.info(serverName + " New message from ECS: " + message.getMsg());

            switch (message.getCommand()) {
                case "START":
                    start();
                    break;
                case "STOP":
                    stop();
                    break;
                case "INIT":
                    initKVServer(message.getMetaData(), message.getCacheSize(), message.getCacheStrategy());
                    break;
                case "UPDATE":
                    update(message.getMetaData());
                    break;
                case "SHUT_DOWN":
                    shutDown(message.getMsg());
                    break;
                case "SEND":
                    serverInitSuccess = true;
                    break;
                case "HEARTBEAT":
                    Stat st = zk.exists("/" + serverName + "/heartbeat", false);
                    zk.setData("/" + serverName + "/heartbeat", (serverName + " " + heartbeatNum).getBytes(), st.getVersion());
                    heartbeatNum++;
                    break;
                default:
                    break;
            }

            zk.exists("/" + serverName + "/message", true);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error processing watcher event!");
        }
    }

    @Override
    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (this.running) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection =
                            new ClientConnection(this, client);
                    new Thread(connection).start();

                    logger.info("Connected to client "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
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
     *
     * @param args contains the port number at args[0], cachesize at args[1], strategy at args[2].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.INFO);
            if (args.length != 2) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cache size> <cache strategy>!");
            } else {
                int port = Integer.parseInt(args[0]);
                KVServer app = new KVServer(port, args[1]);
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
