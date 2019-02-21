package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.IOException;

import ecs.ECSNode;
import ecs.IECSNode;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import client.KVStore;
import shared.messages.ClientSocketListener;
import shared.messages.TextMessage;

public class ECSClient implements IECSClient, ClientSocketListener {

    private static final int HASH_LOWER_BOUND = 0;
    private static final int HASH_UPPER_BOUND = Integer.parseInt("FFFF", 16);
    
    private Map<String, IECSNode> nodes;
    
    @Override
    public boolean start() {
        for (IECSNode node : nodes.values()) {
            sendMessage(node, "start");
        }
        return true;
    }

    @Override
    public boolean stop() {
        for (IECSNode node : nodes.values()) {
            sendMessage(node, "stop");
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        removeAllNodes();
        stop = true;
        System.out.println(PROMPT + "Application exit!");
        return true;
    }

    private IECSNode addNodeInternal(String cacheStrategy, int cacheSize, String[] hash_range) {
        int port = 0;
        
        Collection<IECSNode> _nodes = setupNodes(1, cacheStrategy, cacheSize);
        IECSNode old_node = ((IECSNode[])_nodes.toArray())[0];
        String name = old_node.getNodeName();
        if (name.equals("TEST_USE_HASH")) name = hash_range[1];
        ECSNode _node = new ECSNode(name, old_node.getNodeHost() , old_node.getNodePort(), hash_range);
        
        // initiate ssh call
        Process proc;
        String script = "ssh -n 127.0.0.1 nohup java -jar ms2-server.jar 50000 ERROR &";

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
    
    private boolean update_hash_range(String oldkey, String[] range) {
        if (nodes.containsKey(oldkey)) {
            IECSNode oldnode = nodes.remove(oldkey);
            nodes.put(range[1], new ECSNode(oldnode.getNodeName(), oldnode.getNodeHost(), oldnode.getNodePort(), range));
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
        int i=0;
        
        String[] keyset = (String[]) nodes.keySet().toArray();
        Arrays.sort(keyset);
        
        for (String key : keyset) {
            // check largest
            int current = hexstr_to_int(key);
            if (find_circular_gap(last_number, current) > largest_gap) {
                largest_gap = find_circular_gap(last_number, current);
                largest = i;
            }
            last_number = current;
            i++;
        }
        
        String[] range = {Integer.toHexString((largest_gap - largest + 1)), Integer.toHexString(largest)};
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
        String[] hash_loc = hash_range;
        hash_loc[1] = Integer.toHexString((int) hexstr_to_int((hash_range[1]+ hash_range[0]))/2);
        
        // setup nodes called inside
        IECSNode _node = addNodeInternal(cacheStrategy, cacheSize, hash_loc);
        try {
            awaitNodes(1, 100);
        } catch (Exception e) {
            System.out.println("ECSClient addNode Error" + e.getMessage());
            return null;
        }
        nodes.put(hash_loc[1], _node);
        
        // update old hash range (higher)
        String hash_remain = Integer.toHexString(hexstr_to_int(hash_loc[0]) + 1);
        String[] hash_range_old = hash_range;
        hash_range_old[0] = hash_remain;
        update_hash_range(hash_range[1], hash_range_old);
        
        return _node;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        
        ArrayList<IECSNode> _nodes = new ArrayList<IECSNode>();
        for (int i=0; i<count; i++ ) _nodes.add(addNode(cacheStrategy, cacheSize));
        return _nodes;
        
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        int port = 0;
        // to have hash as the name put "TEST_USE_HASH"
        IECSNode _node = (IECSNode) new ECSNode("TEST_USE_HASH", "127.0.0.1", port, null);
        ArrayList<IECSNode> _nodes = new ArrayList<IECSNode>();
        _nodes.add(_node);
        return _nodes;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return true;
    }
    
    private void shutdownNode(IECSNode node) {
        sendMessage(node, "shutdown");
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        String[] _nodeNames = (String[]) nodeNames.toArray();
        boolean somenotdeleted = false;
        for (int i=0; i<=nodeNames.size(); i++) {
            
            // attempt deletion
            if (nodes.containsKey(_nodeNames[i])) {
                IECSNode _node = nodes.remove(_nodeNames[i]);
                shutdownNode(_node);
            }
            else somenotdeleted = true;
            
        }
        return !somenotdeleted;
    }
    
    public void removeAllNodes() {
        for (IECSNode value: nodes.values())
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
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    
    public void handleNewMessage(TextMessage msg) {
		if (connected) {
                    if(!stop) {
                        System.out.println("Server reply: " + msg.getMsg());
                    }
                    System.out.print(PROMPT);
                } else {

                    if (msg.getMsg().indexOf("Connection to storage server established:") != 0) {
                            connected = true;
                    }

                    System.out.println(msg.getMsg());
                    System.out.print(PROMPT);
		}
    }

    public void handleStatus(ClientSocketListener.SocketStatus status) {
        
            /*
            if(null != status) switch (status) {

            case CONNECTED:
                break;

            case DISCONNECTED:
                System.out.print(PROMPT);
                System.out.println("Connection terminated: ");
                break;

            case CONNECTION_LOST:
                System.out.println("Connection lost: "
                        + serverAddress + " / " + serverPort);
                System.out.print(PROMPT);
                break;

            default:
                break;
            }
            */

    }
        
    private void sendMessage(IECSNode node, String message) {
        String address = node.getNodeHost();
        int port = node.getNodePort();
        KVStore connection = new KVStore(address, port);
        try {
            connection.connect();
        } catch (Exception e) {
            printError("Conenction Failed!");
            return;
        }
        
        connection.addListener(this);
        connection.start();
        
        if(connection.isRunning()) {
                            
            try {
                    connection.sendMessage(new TextMessage(message));
            } catch (IOException e) {
                    printError("Unable to send message!");
                    connection.disconnect();
                    return;
            }

        } else {
            printError("Not connected!");
            return;
        }
    }
    
    // INTERFACE
    
    private void handleCommand(String cmdLine) {
        
                String[] tokens = cmdLine.split("\\s+");

                switch (tokens[0]) {

                    case "help":
                        printHelp();
                        break;
                        
                    case "addNodes":
                        if(tokens.length != 4) {
                            printError("Invalid number of parameters!");
                            break;
                        }
                        
                        addNodes(Integer.parseInt(tokens[0], 10), tokens[2], Integer.parseInt(tokens[1], 10));
                        break;
                        
                    case "removeNode":
                        if(tokens.length != 2) {
                            printError("Invalid number of parameters!");
                            break;
                        }
                        ArrayList<String> list = new ArrayList<String>();
                        list.add(tokens[1]);
                        removeNodes(list);
                        break;
                        
                    case "list":
                        if(tokens.length != 1) {
                            printError("Invalid number of parameters!");
                            break;
                        }
                        
                        for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
                            IECSNode _node = entry.getValue();
                            System.out.print(PROMPT + "Node: " + entry.getKey() + "\t\tHash Range: " + _node.getNodeHashRange()[0] + "-" + _node.getNodeHashRange()[1]);
                            System.out.println("\t\tHost: " + entry.getValue().getNodeName() + ":" + entry.getValue().getNodePort());
                        }
                        
                        break;   
                        
                    case "shutdown":
                        if(tokens.length != 1) {
                            printError("Invalid number of parameters!");
                            break;
                        }
                        shutdown();
                        break; 
                        
                    default:
                        printError("Unknown command");
                        printHelp();
                        break;
                }
    }
    
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
    
    private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
    }
    
    private void printHelp() {
                StringBuilder sb = new StringBuilder();
                sb.append("HELP (Usage):\n");
                sb.append("::::::::::::::::::::::::::::::::");
                sb.append("::::::::::::::::::::::::::::::::\n");
                sb.append("addNode <numberOfNodes>, <cacheSize>, <replacementStrategy>");
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
                ECSClient app = new ECSClient();
                app.run();
    }
}
