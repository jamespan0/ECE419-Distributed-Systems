package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.*;

import logger.LogSetup;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import client.KVCommInterface;
import client.KVStore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.ByteBuffer;

import shared.messages.TextMessage;
import shared.messages.ClientSocketListener;

public class KVClient implements IKVClient, ClientSocketListener, Watcher {
    
        private static Logger logger = Logger.getRootLogger();
        private static final String PROMPT = "StorageClient> ";
        private BufferedReader stdin;
        private KVStore client = null;
        private boolean stop = false;
		private boolean connected = false;
	private boolean testconnected = false;
	private boolean nowatch = false;
		private String modifying = "";

        private String serverAddress;
        private int serverPort;


	private static final String ZK_CONNECT = "127.0.0.1:2181";
	private static final int ZK_TIMEOUT = 2000;

	private ZooKeeper zk;
	private CountDownLatch connectedSignal;


		class Address {
			public String host;
			public int port;

			public Address(String _host, int _port){
				this.host = _host;
				this.port = _port;
			}
		};

		private TreeMap<String, Address> hostTable = new TreeMap<String, Address>();

		public KVClient (){
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

				zk.register(this);
			} catch (Exception e){
				e.printStackTrace();
			}
		}

        @Override public void newConnection(String hostname, int port) throws Exception, UnknownHostException, IOException {
            client = new KVStore(hostname, port);
            client.connect();
            client.addListener(this);
            client.start();
        }

        @Override public KVCommInterface getStore() {

            return (KVCommInterface)client;
        }
		
		@Override public void process(WatchedEvent we) {
			try {
				String changedKey = we.getPath().split("/")[2];
				
				if(!nowatch) {
					Stat st = zk.exists(we.getPath(), true);

					if (changedKey.equals(this.modifying)){
						this.modifying = "";
					} else {

						if (st == null){
							System.out.println("Watched key \"" + changedKey + "\" has been deleted!");
							System.out.print(PROMPT);
						} else {
							System.out.println("Watched key \"" + changedKey + "\" has been modified!");
							System.out.print(PROMPT);
						}
					}
				}

				nowatch = false;
			} catch(Exception e) {
				e.printStackTrace();
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

		private Address getResponsibleServer(String key) {
			Set<String> keySet = hostTable.keySet();

			for (String rangeHigh : keySet) {
				for (int i = 0; i < key.length(); i++) {

					int high;
					int highLength = rangeHigh.length();

					if(i < highLength) {
						high = Integer.parseInt(rangeHigh.substring(i, i + 1), 16);
					} else {
						high = Integer.parseInt(rangeHigh.substring(highLength - 1, highLength), 16);
					}

					int keyTemp = Integer.parseInt(key.substring(i, i + 1), 16);

					if (keyTemp < high) {
						return hostTable.get(rangeHigh);
					} 

					if (keyTemp > high) {
						break;
					}
				}
			}

			return null;
		}


        private void handleCommand(String cmdLine) {
                String[] tokens = cmdLine.split("\\s+");

            switch (tokens[0]) {

                case "quit":
                    stop = true;
                    disconnect();
                    System.out.println(PROMPT + "Application exit!");
                    break;

                case "connect":
                    if(tokens.length == 3) {
                        try{
                            serverAddress = tokens[1];
                            serverPort = Integer.parseInt(tokens[2]);
							if(serverAddress.equals("localhost")){
								serverAddress = "127.0.0.1";
							}
                            newConnection(serverAddress, serverPort);
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
                            printError("Unknown error!");
                            logger.warn("Exception Thrown!", e);
                        }
                    } else {
                        printError("Invalid number of parameters!");
                    }
                    break;

                case "put":
                    if(tokens.length == 3 || tokens.length == 4) {
                        if(client != null && client.isRunning()){
                            
                            StringBuilder msg = new StringBuilder();
                            for(int i = 0; i < 3; i++) {
                                msg.append(tokens[i]);
                                if (i != tokens.length -1 ) {
                                    msg.append(" ");
                                }
                            }

							String watch = "";

							if (tokens.length == 4){
								watch = tokens[3];
							}

							String hashedKey = hashing(tokens[1]);
							Address targetServer = getResponsibleServer(hashedKey);

							if (!targetServer.host.equals(client.address) || targetServer.port != client.port){
								try {
									testconnected = false;
									client.disconnect();
		       	 					newConnection(targetServer.host, targetServer.port);
								} catch (Exception e) {
									logger.error(e);
								}

								long currentTime = System.currentTimeMillis();
	
								System.out.println("Connecting to responsbile server......");

								while (System.currentTimeMillis() - currentTime < 10000 && testconnected == false) {}
							}

							if(testconnected == false) {
								System.out.println("Connection request timed out. Please retry connection.");
							} else {
								sendMessage(msg.toString());
								try{
									if(watch.equals("watch")){
										this.modifying = tokens[1];
										Stat st = zk.exists("/dataWatch/" + tokens[1], false);
				
										if (st == null) {
											zk.create("/dataWatch/" + tokens[1], ByteBuffer.allocate(4).putInt(0).array(),
																				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
										} else {
											zk.setData("/dataWatch/" + tokens[1], ByteBuffer.allocate(4).putInt(0).array(), st.getVersion());
										}

										st = zk.exists("/dataWatch/" + tokens[1], true);
									} else {
										this.modifying = tokens[1];
										Stat st = zk.exists("/dataWatch/" + tokens[1], false);
				
										if (st == null) {
											zk.create("/dataWatch/" + tokens[1], ByteBuffer.allocate(4).putInt(0).array(),
																				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
										} else {
											zk.setData("/dataWatch/" + tokens[1], ByteBuffer.allocate(4).putInt(0).array(), st.getVersion());
										}

										st = zk.exists("/dataWatch/" + tokens[1], false);
									}
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
                            

                        } else {
                            printError("Not connected!");
                        }
                    } else {
                        printError("Syntax Error!");
                        printError("put <key> <value>");
                    }
                    break;

                case "get":
                    if(tokens.length == 2 || tokens.length == 3) {
                        if(client != null && client.isRunning()){

                            StringBuilder msg = new StringBuilder();
                            for(int i = 0; i < tokens.length; i++) {
                                msg.append(tokens[i]);
                                if (i != tokens.length -1 ) {
                                    msg.append(" ");
                                }
                            }

							String watch = "";

							if (tokens.length == 3){
								watch = tokens[2];
							}


							String hashedKey = hashing(tokens[1]);
							Address targetServer = getResponsibleServer(hashedKey);

							if (!targetServer.host.equals(client.address) || targetServer.port != client.port){
								try {
									testconnected = false;
									client.disconnect();
		       	 					newConnection(targetServer.host, targetServer.port);
								} catch (Exception e) {
									logger.error(e);
								}

								long currentTime = System.currentTimeMillis();
	
								System.out.println("Connecting to responsbile server " + targetServer.host + ":" + 
																							targetServer.port + "......");

								while (System.currentTimeMillis() - currentTime < 10000 && testconnected == false) {}
							}

							if(testconnected == false) {
								System.out.println("Connection request timed out. Please retry connection.");
							} else {
								sendMessage(msg.toString());
								try {
										Stat st = zk.exists("/dataWatch/" + tokens[1], false);
									if (watch.equals("nowatch")){
										nowatch = true;
											zk.setData("/dataWatch/" + tokens[1], ByteBuffer.allocate(4).putInt(0).array(), st.getVersion());
									} else if(watch.equals("watch")){
				
										if (st != null) {
											st = zk.exists("/dataWatch/" + tokens[1], true);
										}
									}
								} catch (Exception e){
									e.printStackTrace();
								}
							}
                            
                        } else {
                            printError("Not connected!"); 
                        }
                    } else {
                        printError("Syntax Error!");
                        printError("get <key>");
                    }
                    break;

                case "disconnect":
                    disconnect();
                    break;

                case "logLevel":
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
            
        private void sendMessage(String msg){
                try {
                        client.sendMessage(new TextMessage(msg));
                } catch (IOException e) {
                        printError("Unable to send message!");
                        disconnect();
                }
        }

        private void disconnect() {
		connected = false;

                if(client != null) {
                        client.closeConnection();
                        client = null;
                }
        }

        private void printHelp() {
                StringBuilder sb = new StringBuilder();
                sb.append("HELP (Usage):\n");
                sb.append("::::::::::::::::::::::::::::::::");
                sb.append("::::::::::::::::::::::::::::::::\n");
                sb.append("connect <host> <port>");
                sb.append("\t establishes a connection to a server\n");
                sb.append("put <key> <value>");
                sb.append("\t\t stores a key value pair on the server \n");
                sb.append("get <key>");
                sb.append("\t\t requests the value of specified key from the server \n");
                sb.append("disconnect");
                sb.append("\t\t\t disconnects from the server \n");

                sb.append("logLevel");
                sb.append("\t\t\t changes the logLevel \n");
                sb.append("\t\t\t\t ");
                sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

                sb.append("quit ");
                sb.append("\t\t\t exits the program");
                System.out.println(sb.toString());
        }

        private void printPossibleLogLevels() {
                System.out.println(PROMPT 
                                + "Possible log levels are:");
                System.out.println(PROMPT 
                                + LogSetup.getPossibleLogLevels());
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


		// returns hash of String
		public String hashing(String message) {
		    MessageDigest messageDigest;
		    try {
		        messageDigest = MessageDigest.getInstance("MD5");
		        messageDigest.update(message.getBytes());
		        byte[] messageDigestMD5 = messageDigest.digest();
		        StringBuffer stringBuffer = new StringBuffer();
		        for(byte bytes : messageDigestMD5) {
		            stringBuffer.append(String.format("%02x",bytes & 0xff));
		        } 

		        return stringBuffer.toString();

		    } catch(NoSuchAlgorithmException exception) {
		        exception.printStackTrace(); 
		    }
		    return "";
		}


        @Override public void handleNewMessage(TextMessage msg) {
			String[] tokens = msg.getMsg().split(" ");
	
			if(tokens[0].equals("CONNECTION_SUCCESS") || tokens[0].equals("SERVER_NOT_RESPONSIBLE")) {

				if (tokens[0].equals("CONNECTION_SUCCESS")) {
					testconnected = true;
        			System.out.println("Connection to storage server established!");
				} else if (tokens[0].equals("SERVER_NOT_RESPONSIBLE")) {
					System.out.println("Server reply: SERVER_NOT_RESPONSIBLE");
				}

				hostTable.clear();

				JSONObject JSONmetaData = new JSONObject(tokens[1]);

				Set<String> keySet = JSONmetaData.keySet();

				for	(String key : keySet) {
					if (!key.equals("transfer")){
						JSONObject node = JSONmetaData.getJSONObject(key);
						String range[] = {node.getString("rangeLow"), node.getString("rangeHigh")};
						String host = node.getString("host");
						int port = node.getInt("port");

						hostTable.put(range[1], new Address(host, port));
					}
				}
				
			} else {
				System.out.println("Server reply: " + msg.getMsg());
				if (tokens[0].equals("PUT_DELETE")) {
					try {
						Stat st = zk.exists("/dataWatch/" + tokens[1], false);
						zk.delete("/dataWatch/" + tokens[1], st.getVersion());
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if ((tokens[0].equals("PUT_SUCCESS") || tokens[0].equals("PUT_UPDATE")) && tokens[1].equals(this.modifying)){
					this.modifying = "";
				}
			}
			
            System.out.print(PROMPT);
        }

        @Override public void handleStatus(SocketStatus status) {
                if(null != status) switch (status) {

                case CONNECTED:
                    break;

                case DISCONNECTED:
                    System.out.print(PROMPT);
                    System.out.println("Connection terminated: "
                            + serverAddress + " / " + serverPort);
                    break;

                case CONNECTION_LOST:
                    System.out.println("Connection lost: "
                            + serverAddress + " / " + serverPort);
                    System.out.print(PROMPT);
                    break;

                default:
                    break;
            }

        }

        private void printError(String error){
                System.out.println(PROMPT + "Error! " +  error);
        }

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
                        new LogSetup("logs/client.log", Level.OFF);
                        KVClient app = new KVClient();
                        app.run();
                } catch (IOException e) {
                        System.out.println("Error! Unable to initialize logger!");
                        e.printStackTrace();
                        System.exit(1);
                }
    }
}
