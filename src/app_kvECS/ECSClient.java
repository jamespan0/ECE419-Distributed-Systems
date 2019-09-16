package app_kvECS;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import java.nio.ByteBuffer;

import java.io.*;

import ecs.ECSNode;
import ecs.IECSNode;

import client.KVStore;
import shared.messages.ClientSocketListener;
import shared.messages.KVAdminMessage;
import shared.messages.TextMessage;

import logger.LogSetup;
import org.apache.log4j.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;

import org.json.*;

public class ECSClient implements IECSClient, ClientSocketListener, Watcher {

    private static Logger logger = Logger.getRootLogger();

    private static final int HASH_LOWER_BOUND = 0;
    private static final int HASH_UPPER_BOUND = Integer.parseInt("FFFF", 16);

    int activeServers;

    private static final String ZK_CONNECT = "127.0.0.1:2181";
    private static final int ZK_TIMEOUT = 2000;

    private ZooKeeper zk;
    private CountDownLatch connectedSignal;

    private Map<String, IECSNode> nodes;
    private Map<String, String[]> replicaOf = new HashMap<String, String[]>();
    private Map<String, String[]> replicas = new HashMap<String, String[]>();
    private Queue<IECSNode> availableNodes = new LinkedList<>();

    private boolean transferSuccess = false;

    ECSServerMonitor serverMonitor;

    public ECSClient(String configFileName) {
        activeServers = 0;

        serverMonitor = new ECSServerMonitor(this);
        serverMonitor.running = true;
        new Thread(serverMonitor).start();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(configFileName)));

            String line;

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(" ");

                ECSNode temp = new ECSNode(tokens[0], tokens[1], Integer.parseInt(tokens[2]), null);

                availableNodes.add(temp);
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

            Stat st = zk.exists("/activeNodes", false);

            if (st == null) {
                zk.create("/activeNodes", ByteBuffer.allocate(4).putInt(0).array(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData("/activeNodes", ByteBuffer.allocate(4).putInt(0).array(), st.getVersion());
            }

            st = zk.exists("/ecsMessages", false);

            if (st == null) {
                zk.create("/ecsMessages", "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData("/ecsMessages", "".getBytes(), st.getVersion());
                st = zk.exists("/ecsMessages", this);
            }

            st = zk.exists("/dataWatch", false);

            if (st == null) {
                zk.create("/dataWatch", "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData("/dataWatch", "".getBytes(), st.getVersion());
                st = zk.exists("/dataWatch", this);
            }

            logger.info("Znodes initialized");

        } catch (KeeperException | IOException | InterruptedException e) {
            logger.error(e);
        }

    }

    @Override
    public void process(WatchedEvent we) {
        try {
            String message = new String(zk.getData("/ecsMessages", this, null));

            String[] tokens = message.split(" ");

            if (message.equals("TRANSFER_SUCCESS")) {
                logger.info("Server data transfer successful");
                transferSuccess = true;
            } else if (!message.equals("")) {
                logger.info("Received Message: " + message);
                System.out.print(PROMPT);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e);
        }
    }


    @Override
    public boolean start() {
        if (nodes.isEmpty()) {
            System.out.println(PROMPT + "No Nodes!");
            return false;
        }
        for (IECSNode node : nodes.values()) {
            try {
                Stat st = zk.exists("/" + node.getNodeName() + "/message", true);

                zk.setData("/" + node.getNodeName() + "/message", "START".getBytes(), st.getVersion());

            } catch (KeeperException | InterruptedException e) {

            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        if (nodes.isEmpty()) {
            System.out.println(PROMPT + "No Nodes!");
            return false;
        }
        for (IECSNode node : nodes.values()) {
            try {
                Stat st = zk.exists("/" + node.getNodeName() + "/message", true);

                zk.setData("/" + node.getNodeName() + "/message", "STOP".getBytes(), st.getVersion());
            } catch (KeeperException | InterruptedException e) {

            }
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        try {
            serverMonitor.running = false;
            List<String> children = zk.getChildren("/activeNodes", false);

            for (String child : children) {
                Stat st = zk.exists("/activeNodes/" + child, false);
                zk.delete("/activeNodes/" + child, st.getVersion());
            }

            children = zk.getChildren("/dataWatch", false);

            for (String child : children) {
                Stat st = zk.exists("/dataWatch/" + child, false);
                zk.delete("/dataWatch/" + child, st.getVersion());
            }

            Process proc;
            String script = "pkill -f m2-server";
            Runtime run = Runtime.getRuntime();
            proc = run.exec(script);

            removeAllNodes();
            stop = true;
            System.out.println(PROMPT + "Application exit!");

        } catch (Exception e) {
            logger.error(e);
        }

        return true;
    }

    private IECSNode addNodeInternal(String cacheStrategy, int cacheSize, String[] hash_range) {

        IECSNode old_node = null;
        if (availableNodes.peek() != null) {
            old_node = (IECSNode) availableNodes.remove();
        } else {
            return null;
        }


        String name = old_node.getNodeName();

        ECSNode _node = new ECSNode(name, old_node.getNodeHost(), old_node.getNodePort(), hash_range);

        // initiate ssh call
        Process proc;
        String script = "ssh -n " + _node.getNodeHost() + " nohup java -jar m2-server.jar " + _node.getNodePort() + " " + _node.getNodeName() + " &";

        Runtime run = Runtime.getRuntime();
        try {
            proc = run.exec(script);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (IECSNode) _node;
    }

    private int hexstr_to_int(String hex) {
        return Integer.parseInt(hex, 16);
    }

    private String getMetadata(boolean transfer) {
        JSONObject metaData = new JSONObject();

        for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
            IECSNode _node = entry.getValue();

            JSONObject nodeData = new JSONObject();

            String[] replicas_ = replicas.get(_node.getNodeName());
            String[] isReplica = replicaOf.get(_node.getNodeName());

            nodeData.put("host", _node.getNodeHost());
            nodeData.put("port", _node.getNodePort());
            nodeData.put("rangeLow", _node.getNodeHashRange()[0]);
            nodeData.put("rangeHigh", _node.getNodeHashRange()[1]);
            if (replicas_ != null) {
                nodeData.put("replicas", replicas_);
            } else {
                String[] empty = {"", ""};
                nodeData.put("replicas", empty);
            }
            if (isReplica != null) {
                nodeData.put("replicaOf", isReplica);
            } else {
                String[] empty = {"", ""};
                nodeData.put("replicaOf", empty);
            }

            metaData.put("transfer", transfer);
            metaData.put(_node.getNodeName(), nodeData);
        }

        return metaData.toString();
    }

    private void updateReplicas(String coordinator) {
        String[] replicas_ = new String[2];
        replicas_[0] = "";
        replicas_[1] = "";
        int coordinatorInt = Integer.parseInt(coordinator, 16);

        // only 1 node, no replicas
        if (nodes.size() > 1) {
            int low1 = 0;
            int low2 = 0;
            int next1 = -1;
            int next2 = -1;
            for (String key : nodes.keySet()) {
                if (!key.equals(coordinator)) {
                    int keyInt = Integer.parseInt(key, 16);
                    if (low1 == 0) {
                        low1 = keyInt;
                        low2 = keyInt;
                    }

                    if (keyInt < low1) {
                        if (low2 != low1) {
                            low2 = low1;
                        }

                        low1 = keyInt;
                    }

                    if (low2 == low1) {
                        low2 = keyInt;
                    }

                    if (keyInt < low2 && keyInt != low1) {
                        low2 = keyInt;
                    }

                    if (keyInt > coordinatorInt && next1 == -1) {
                        next1 = keyInt;
                        next2 = keyInt;
                    }

                    if (keyInt > coordinatorInt && keyInt < next1) {
                        if (next2 != next1) {
                            next2 = next1;
                        }

                        next1 = keyInt;
                    }


                    if (next2 == next1 && keyInt > next1) {
                        next2 = keyInt;
                    }

                    if (keyInt > next1 && keyInt < next2) {
                        next2 = keyInt;
                    }
                }
            }


            if (next1 == -1) {
                next1 = low1;
                next2 = low2;
            } else if (next2 == -1 || next2 == next1) {
                next2 = low1;
            }

            if (next1 != 0) {
                replicas_[0] = Integer.toHexString(next1);
            }

            if (next2 != 0 && next2 != next1) {
                replicas_[1] = Integer.toHexString(next2);
            }

        }

        String coordinatorName = nodes.get(coordinator).getNodeName();

        if (!replicas_[0].equals("")) {
            String[] temp = replicaOf.get(nodes.get(replicas_[0]).getNodeName());

            if (temp != null) {

                if (!temp[0].equals(coordinatorName)) {
                    temp[1] = coordinatorName;
                }
            } else {
                temp = new String[2];
                temp[0] = coordinatorName;
                temp[1] = "";
            }

            replicaOf.put(nodes.get(replicas_[0]).getNodeName(), temp);
        }

        if (!replicas_[1].equals("")) {
            String[] temp = replicaOf.get(nodes.get(replicas_[1]).getNodeName());

            if (temp != null) {

                if (!temp[0].equals(coordinatorName)) {
                    temp[1] = coordinatorName;
                }
            } else {
                temp = new String[2];
                temp[0] = coordinatorName;
                temp[1] = "";
            }

            replicaOf.put(nodes.get(replicas_[1]).getNodeName(), temp);
        }

        if (!replicas_[0].equals("")) {
            replicas_[0] = nodes.get(replicas_[0]).getNodeName();
        }

        if (!replicas_[1].equals("")) {
            replicas_[1] = nodes.get(replicas_[1]).getNodeName();
        }

        replicas.put(nodes.get(coordinator).getNodeName(), replicas_);
    }

    private boolean update_hash_range(String oldkey, String[] range, String newNode, int cacheSize,
                                      String cacheStrategy, boolean firstNode) {
        if (nodes.containsKey(oldkey)) {
            IECSNode oldnode = nodes.remove(oldkey);

            String name = oldnode.getNodeName();

            nodes.put(range[1], new ECSNode(oldnode.getNodeName(), oldnode.getNodeHost(), oldnode.getNodePort(), range));

            for (Map.Entry<String, IECSNode> temp : nodes.entrySet()) {
                updateReplicas(temp.getValue().getNodeHashRange()[1]);
            }
            try {
                if (!firstNode) {
                    int timeout = 1000;
                    long currentTime = System.currentTimeMillis();

                    while (System.currentTimeMillis() - currentTime < timeout) {

                    }

                    zk.setData("/" + newNode + "/message", ("INIT " + cacheSize + " " + cacheStrategy + " " +
                            getMetadata(false)).getBytes(), zk.exists("/" + newNode + "/message", true).getVersion());

                    //add initialized server to monitor
                    serverMonitor.testActive.add(newNode);
                }
                zk.setData("/" + name + "/message", ("UPDATE " + getMetadata(true)).getBytes(),
                        zk.exists("/" + name + "/message", true).getVersion());

                //WAIT FOR TRANSFER COMPLETION
                int timeout = 10000;
                long currentTime = System.currentTimeMillis();

                logger.info("Waiting for transfer of server data...");

                while (System.currentTimeMillis() - currentTime < timeout && !transferSuccess) {
                    //getData Progress
                }

                transferSuccess = false;

                for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
                    String nodeName = entry.getValue().getNodeName();

                    if (!nodeName.equals(name) && !nodeName.equals(newNode)) {
                        zk.setData("/" + nodeName + "/message", ("UPDATE " + getMetadata(false)).getBytes(),
                                zk.exists("/" + nodeName + "/message", true).getVersion());

                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
            }

            return true;
        }
        return false;
    }

    private String[] hash_insert_loc() {
        if (nodes.size() == 0) {
            String[] range = {Integer.toHexString(HASH_LOWER_BOUND), Integer.toHexString(HASH_UPPER_BOUND)};
            return range;
        }

        int largest = 0;
        int largest_gap = 0;
        int last_number = 0;

        String[] keyset = nodes.keySet().toArray(new String[0]);
        Arrays.sort(keyset);

        for (String key : keyset) {
            // check largest
            int current = hexstr_to_int(key);
            if (find_circular_gap(last_number, current) > largest_gap) {
                largest_gap = find_circular_gap(last_number, current);
                largest = current;
            }
            last_number = current;
        }

        String[] range = {Integer.toHexString((largest - largest_gap)), Integer.toHexString(largest)};
        return range;
    }

    private int find_circular_gap(int lower, int upper) {
        if (lower > upper) {
            return upper + HASH_UPPER_BOUND - lower + 1;
        }
        return upper - lower;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {

        String[] hash_range = hash_insert_loc();

        // update new hash range (lower)
        String[] hash_loc = new String[2];
        hash_loc[0] = hash_range[0];
        hash_loc[1] = hash_range[1];

        // check if first node
        if (nodes.size() != 0)
            hash_loc[1] = Integer.toHexString((int) (hexstr_to_int(hash_range[0]) + hexstr_to_int(hash_range[1])) / 2);
        if (!hash_loc[0].equals("0")) {
            hash_loc[0] = Integer.toHexString(Integer.parseInt(hash_loc[0], 16) + 1);
        }

        // add new node with the range lower - mid
        IECSNode _node = addNodeInternal(cacheStrategy, cacheSize, hash_loc);

        if (_node == null) {
            System.out.println("Error! No more available servers!");
            return null;
        }

        try {
            if (awaitNodes(1, 10000)) {
                activeServers++;

                nodes.put(hash_loc[1], _node);

                boolean firstNode = true;


                if (nodes.size() == 1) {
                    zk.setData("/" + _node.getNodeName() + "/message", ("INIT " + cacheSize + " " + cacheStrategy + " " +
                                    getMetadata(false)).getBytes(),
                            zk.exists("/" + _node.getNodeName() + "/message", true).getVersion());
                    //add initialized server to monitor
                    serverMonitor.testActive.add(_node.getNodeName());
                } else {
                    firstNode = false;
                }

                // update old node with the range mid - higher
                if (nodes.size() != 1) {
                    // update old hash range (higher)
                    hash_range[0] = Integer.toHexString(hexstr_to_int(hash_loc[1]) + 1);
                    update_hash_range(hash_range[1], hash_range, _node.getNodeName(), cacheSize, cacheStrategy, firstNode);
                }
            }
        } catch (Exception e) {
            System.out.println("ECSClient addNode Error" + e);
            e.printStackTrace();
            return null;
        }
        return _node;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {

        ArrayList<IECSNode> _nodes = new ArrayList<IECSNode>();

        for (int i = 0; i < count; i++) {
            IECSNode node = addNode(cacheStrategy, cacheSize);
            if (node == null) {
                return null;
            }
            _nodes.add(node);
        }

        return _nodes;

    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        IECSNode _node = null;
        ArrayList<IECSNode> _nodes = new ArrayList<IECSNode>();

        if (availableNodes.peek() != null) {
            _node = (IECSNode) availableNodes.remove();
            _nodes.add(_node);
        }

        return _nodes;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {

        long currentTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - currentTime < timeout) {
            List<String> activeNodes = zk.getChildren("/activeNodes", true);

            if (activeNodes.size() == activeServers + count) {
                return true;
            }
        }

        return false;
    }

    private void shutdownNode(IECSNode node) {

        try {
            Stat st = zk.exists("/" + node.getNodeName() + "/message", true);

            zk.setData("/" + node.getNodeName() + "/message", "SHUT_DOWN".getBytes(), st.getVersion());
        } catch (KeeperException | InterruptedException e) {

        }

    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        return false;
    }

    ;

    public boolean removeNodes(Collection<String> nodeNames, boolean failed) {
        String[] _nodeNames = nodeNames.toArray(new String[nodeNames.size()]);
        boolean somenotdeleted = false;

        for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
            IECSNode _node = entry.getValue();

            if (_node.getNodeName().equals(_nodeNames[0])) {
                //shutdownNode(_node);

                int rangeLow = Integer.parseInt(_node.getNodeHashRange()[0], 16);
                int rangeHigh = Integer.parseInt(_node.getNodeHashRange()[1], 16);

                IECSNode nodeAbove = _node;
                IECSNode nodeBelow = _node;

                String transferHost;
                int transferPort;

                nodes.remove(entry.getKey());
                if (rangeLow == 0) {
                    int temp = rangeHigh + 1;

                    for (Map.Entry<String, IECSNode> entry_ : nodes.entrySet()) {
                        IECSNode tempNode = entry_.getValue();
                        int tempRangeLow = Integer.parseInt(tempNode.getNodeHashRange()[0], 16);

                        if (tempRangeLow == temp) {
                            nodeAbove = tempNode;
                            break;
                        }
                    }

                    transferHost = nodeAbove.getNodeHost();
                    transferPort = nodeAbove.getNodePort();
                    String[] newRange = {_node.getNodeHashRange()[0], nodeAbove.getNodeHashRange()[1]};
                    nodeAbove.setNodeHashRange(newRange);
                } else if (rangeHigh == 65535) {
                    int temp = rangeLow - 1;
                    nodeBelow = nodes.get(Integer.toHexString(temp));

                    transferHost = nodeBelow.getNodeHost();
                    transferPort = nodeBelow.getNodePort();
                    String[] newRange = {nodeBelow.getNodeHashRange()[0], _node.getNodeHashRange()[1]};
                    nodeBelow = nodes.remove(nodeBelow.getNodeHashRange()[1]);
                    IECSNode newNodeBelow = new ECSNode(nodeBelow.getNodeName(), nodeBelow.getNodeHost(),
                            nodeBelow.getNodePort(), newRange);
                    nodes.put(newRange[1], newNodeBelow);
                } else {
                    int temp = rangeHigh + 1;

                    for (Map.Entry<String, IECSNode> entry_ : nodes.entrySet()) {
                        IECSNode tempNode = entry_.getValue();
                        int tempRangeLow = Integer.parseInt(tempNode.getNodeHashRange()[0], 16);

                        if (tempRangeLow == temp) {
                            nodeAbove = tempNode;
                            break;
                        }
                    }

                    temp = rangeLow - 1;
                    nodeBelow = nodes.get(Integer.toHexString(temp));

                    int checkRange1 = Integer.parseInt(nodeAbove.getNodeHashRange()[1], 16) -
                            Integer.parseInt(nodeAbove.getNodeHashRange()[0], 16);
                    int checkRange2 = Integer.parseInt(nodeBelow.getNodeHashRange()[1], 16) -
                            Integer.parseInt(nodeBelow.getNodeHashRange()[0], 16);

                    if (checkRange1 <= checkRange2) {
                        transferHost = nodeAbove.getNodeHost();
                        transferPort = nodeAbove.getNodePort();
                        String[] newRange = {_node.getNodeHashRange()[0], nodeAbove.getNodeHashRange()[1]};
                        nodeAbove.setNodeHashRange(newRange);
                    } else {
                        transferHost = nodeBelow.getNodeHost();
                        transferPort = nodeBelow.getNodePort();
                        String[] newRange = {nodeBelow.getNodeHashRange()[0], _node.getNodeHashRange()[1]};
                        nodeBelow = nodes.remove(nodeBelow.getNodeHashRange()[1]);
                        IECSNode newNodeBelow = new ECSNode(nodeBelow.getNodeName(), nodeBelow.getNodeHost(), nodeBelow.getNodePort(), newRange);
                        nodes.put(newRange[1], newNodeBelow);

                    }
                }
                try {
                    if (!failed) {
                        zk.setData("/" + _node.getNodeName() + "/message", ("SHUT_DOWN " + transferHost + " " + transferPort).getBytes(), zk.exists("/" + _node.getNodeName() + "/message", true).getVersion());

                        int timeout = 10000;
                        long currentTime = System.currentTimeMillis();

                        logger.info("Waiting for transfer of server data...");

                        while (System.currentTimeMillis() - currentTime < timeout && !transferSuccess) {
                            //getData Progress
                        }

                        transferSuccess = false;
                    }

                    String path = "/" + _nodeNames[0];

                    zk.delete("/activeNodes" + path, zk.exists("/activeNodes" + path, false).getVersion());
                    activeServers--;
                    zk.delete(path + "/message", zk.exists(path + "/message", false).getVersion());
                    zk.delete(path + "/heartbeat", zk.exists(path + "/heartbeat", false).getVersion());
                    zk.delete(path, zk.exists(path, false).getVersion());

                    ECSNode temp = new ECSNode(_node.getNodeName(), _node.getNodeHost(), _node.getNodePort(), null);

                    if (!failed) {
                        availableNodes.add(temp);
                    }


                    for (Map.Entry<String, IECSNode> entry_ : nodes.entrySet()) {
                        String nodeName = entry_.getValue().getNodeName();

                        zk.setData("/" + nodeName + "/message", ("UPDATE " + getMetadata(false)).getBytes(),
                                zk.exists("/" + nodeName + "/message", true).getVersion());
                    }
                } catch (KeeperException | InterruptedException e) {
                    logger.error("Error! Znode deletion failed: " + e);
                    e.printStackTrace();
                }
                return true;
            }
        }


        return false;
    }

    public void replace(String failedNode) {
        try {
            String path = "/" + failedNode;

            zk.delete("/activeNodes" + path, zk.exists("/activeNodes" + path, false).getVersion());
            activeServers--;
            zk.delete(path + "/message", zk.exists(path + "/message", false).getVersion());
            zk.delete(path + "/heartbeat", zk.exists(path + "/heartbeat", false).getVersion());
            zk.delete(path, zk.exists(path, false).getVersion());

            for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
                IECSNode node = entry.getValue();

                if (node.getNodeName().equals(failedNode)) {
                    IECSNode old_node = null;
                    if (availableNodes.peek() != null) {
                        old_node = (IECSNode) availableNodes.remove();

                        IECSNode newNode = new ECSNode(failedNode, old_node.getNodeHost(), old_node.getNodePort(), node.getNodeHashRange());
                        String range = node.getNodeHashRange()[1];
                        nodes.remove(range);

                        Process proc;
                        String script = "ssh -n " + newNode.getNodeHost() + " nohup java -jar m2-server.jar " + newNode.getNodePort() + " " + failedNode + " &";

                        Runtime run = Runtime.getRuntime();
                        proc = run.exec(script);

                        nodes.put(range, newNode);

                        int timeout = 1000;
                        long currentTime = System.currentTimeMillis();

                        while (System.currentTimeMillis() - currentTime < timeout) {

                        }

                        zk.setData("/" + failedNode + "/message", ("INIT 10 FIFO " +
                                getMetadata(false)).getBytes(), zk.exists("/" + failedNode + "/message", true).getVersion());

                        //add initialized server to monitor
                        serverMonitor.testActive.add(failedNode);


                        for (Map.Entry<String, IECSNode> entry2 : nodes.entrySet()) {
                            String nodeName = entry2.getValue().getNodeName();

                            if (!nodeName.equals(failedNode)) {
                                zk.setData("/" + nodeName + "/message", ("UPDATE " + getMetadata(false)).getBytes(),
                                        zk.exists("/" + nodeName + "/message", true).getVersion());

                            }
                        }


                    } else {
                        ArrayList<String> list = new ArrayList<String>();
                        list.add(failedNode);
                        removeNodes(list, true);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    public void removeAllNodes() {
        for (IECSNode value : nodes.values())
            shutdownNode(value);
        nodes.clear();
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return nodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    // Network Conenction
    boolean connected = false;
    private boolean stop = false;
    public static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;

    public void handleNewMessage(TextMessage msg) {
    }

    public void handleStatus(ClientSocketListener.SocketStatus status) {
    }

    // INTERFACE

    private void handleCommand(String cmdLine) {

        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {

            case "help":
                printHelp();
                break;

            case "addNodes":
                if (tokens.length != 4) {
                    printError("Invalid number of parameters!");
                    break;
                }

                addNodes(Integer.parseInt(tokens[1], 10), tokens[3], Integer.parseInt(tokens[2], 10));
                break;

            case "removeNode":
                if (tokens.length != 2) {
                    printError("Invalid number of parameters!");
                    break;
                }
                ArrayList<String> list = new ArrayList<String>();
                list.add(tokens[1]);
                removeNodes(list, false);
                break;

            case "list":
                if (tokens.length != 1) {
                    printError("Invalid number of parameters!");
                    break;
                }

                if (nodes == null) {
                    System.out.println(PROMPT + "No Nodes!");
                    break;
                }

                if (nodes.isEmpty()) {
                    System.out.println(PROMPT + "No Nodes!");
                    break;
                }

                for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {

                    IECSNode _node = entry.getValue();
                    System.out.print(PROMPT + "Node: " + entry.getValue().getNodeName() +
                            "\t\tHash Range: " + _node.getNodeHashRange()[0] + "-" + _node.getNodeHashRange()[1]);

                    System.out.println("\t\tHost: " + entry.getValue().getNodeHost() + ":" + entry.getValue().getNodePort());

                }

                break;

            case "shutdown":
                shutdown();
                break;

            case "start":
                start();
                break;

            case "stop":
                stop();
                break;

            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }

    public void run() {
        while (!stop) {
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

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append("addNodes <numberOfNodes> <cacheSize> <replacementStrategy>");
        sb.append("\t adds a node\n");
        sb.append("removeNode <name>");
        sb.append("\t removes a node\n");
        sb.append("list");
        sb.append("\t lists all nodes\n");
        sb.append("start");
        sb.append("\t starts all KVServers\n");
        sb.append("stop");
        sb.append("\t stops all KVServers\n");
        sb.append("shutdown");
        sb.append("\t terminates the entire distributed storage server system.\n");
        System.out.println(sb.toString());
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.INFO);
            ECSClient app = new ECSClient(args[0]);
            app.nodes = new HashMap<String, IECSNode>();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
