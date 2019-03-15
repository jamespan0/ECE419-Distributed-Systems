package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import java.nio.ByteBuffer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

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

	public ECSClient() {
			activeServers = 0;

            
			try {
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
	
				logger.info("Znode /activeNodes initialized");

			} catch (KeeperException | IOException | InterruptedException e) {
				logger.error(e);
			}
            
	}

	@Override
	public void process(WatchedEvent we) {
		try {
			String message = new String(zk.getData("/ecsMessages", this, null));

			if (message != "") {
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
            //sendMessage(node, "START");

			try {
				Stat st = zk.exists("/TEST_USE_HASH/message", true);

				zk.setData("/TEST_USE_HASH/message", "START".getBytes(), st.getVersion());
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
            //sendMessage(node, "STOP");

			try {
				Stat st = zk.exists("/TEST_USE_HASH/message", true);

				zk.setData("/TEST_USE_HASH/message", "STOP".getBytes(), st.getVersion());
			} catch (KeeperException | InterruptedException e) {

			}
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
        IECSNode old_node = (IECSNode) (_nodes.toArray())[0];
        String name = old_node.getNodeName();
        if (name.equals("TEST_USE_HASH")) name = hash_range[1];
        ECSNode _node = new ECSNode(name, old_node.getNodeHost() , old_node.getNodePort(), hash_range);
        
        // initiate ssh call
        Process proc;
        String script = "ssh -n " + _node.getNodeHost() + " nohup java -jar m2-server.jar " + _node.getNodePort() + " " + Integer.toString(cacheSize) + " " + cacheStrategy + " &";
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
                        hash_loc[1] = Integer.toHexString((int) (hexstr_to_int(hash_range[0]) + hexstr_to_int(hash_range[1]))/2);
                    
		    // add new node with the range lower - mid
		    IECSNode _node = addNodeInternal(cacheStrategy, cacheSize, hash_loc);
		    try {
		        if (awaitNodes(1, 3000)) {
					activeServers++;
				}
		    } catch (Exception e) {
		        System.out.println("ECSClient addNode Error" + e.getMessage());
		        return null;
		    }
		    nodes.put(hash_loc[1], _node);
		    
                    // update old node with the range mid - higher
                    if (nodes.size() != 1) {
                        // update old hash range (higher)
                        hash_range[0] = Integer.toHexString(hexstr_to_int(hash_loc[1]) + 1);
                        update_hash_range(hash_range[1], hash_range);
                    }

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
        int port = 8000;
        // to have hash as the name put "TEST_USE_HASH"
        // Get from available nodes with ecs.config
        IECSNode _node = (IECSNode) new ECSNode("TEST_USE_HASH", "127.0.0.1", port, null);
        ArrayList<IECSNode> _nodes = new ArrayList<IECSNode>();
        
        
        _nodes.add(_node);
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
        //sendMessage(node, "SHUT_DOWN");

			try {
				Stat st = zk.exists("/TEST_USE_HASH/message", true);

				zk.setData("/TEST_USE_HASH/message", "SHUT_DOWN".getBytes(), st.getVersion());
			} catch (KeeperException | InterruptedException e) {

			}
        
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        String[] _nodeNames = nodeNames.toArray(new String[nodeNames.size()]);
        boolean somenotdeleted = false;
        for (int i=0; i<nodeNames.size(); i++) {
            System.out.println(i);
            // attempt deletion
            if (nodes.containsKey(_nodeNames[i])) {
                IECSNode _node = nodes.remove(_nodeNames[i]);
                shutdownNode(_node);

				/*try {
					String path = "/" + _nodeNames[i];

					Stat st = zk.exists(path, false);
					zk.delete(path, st.getVersion());
				} catch (KeeperException | InterruptedException e) {
					logger.error("Error! Znode deletion failed.");
				}*/
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
    

    public void handleNewAdminMessage(KVAdminMessage msg) {
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
            printError("Connection Failed!");
            return;
        }
        
        connection.addListener(this);
        connection.start();
        
        if(connection.isRunning()) {
                            
            try {
                    connection.sendAdminMessage(new KVAdminMessage(message));
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
                        
                        addNodes(Integer.parseInt(tokens[1], 10), tokens[3], Integer.parseInt(tokens[2], 10));
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
                            System.out.print(PROMPT + "Node: " + entry.getKey() + "\t\tHash Range: " + _node.getNodeHashRange()[0] + "-" + _node.getNodeHashRange()[1]);
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
            
            // Get file from command line arg
            
            
            ECSClient app = new ECSClient();
            app.nodes = new HashMap<String, IECSNode>();
            app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
