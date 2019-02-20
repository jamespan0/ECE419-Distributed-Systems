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

public class ECSClient implements IECSClient {

    private Map<String, IECSNode> nodes;
    
    @Override
    public boolean start() {
        return false;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public boolean shutdown() {
        return false;
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
            String[] range = {"0000", "FFFF"};
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
            return upper + hexstr_to_int("FFFF") - lower + 1;
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

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        String[] _nodeNames = (String[]) nodeNames.toArray();
        boolean somenotdeleted = false;
        for (int i=0; i<=nodeNames.size(); i++) {
            
            boolean deleted = false;
            nodes.remove(_nodeNames[i]);
            
            if (!deleted) somenotdeleted = true;
        }
        return !somenotdeleted;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return nodes;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    // INTERFACE
    private static final String PROMPT = "StorageClient> ";
    private BufferedReader stdin;
    private boolean stop = false;
    
    private void handleCommand(String cmdLine) {
        
                String[] tokens = cmdLine.split("\\s+");

                switch (tokens[0]) {

                    case "quit":
                        stop = true;
                        System.out.println(PROMPT + "Application exit!");
                        break;

                    case "help":
                        printHelp();
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
                sb.append("addNode ");
                sb.append("\t adds a node\n");

                sb.append("quit ");
                sb.append("\t\t\t exits the program");
                System.out.println(sb.toString());
    }
    
    public static void main(String[] args) {
                ECSClient app = new ECSClient();
                app.run();
    }
}
