package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;

import shared.messages.TextMessage;
import shared.messages.ClientSocketListener;

public class KVClient implements IKVClient, ClientSocketListener {
    
        private static Logger logger = Logger.getRootLogger();
        private static final String PROMPT = "StorageClient> ";
        private BufferedReader stdin;
        private KVStore client = null;
        private boolean stop = false;
		private boolean connected = false;

        private String serverAddress;
        private int serverPort;

        @Override public void newConnection(String hostname, int port) throws Exception, UnknownHostException, IOException {
            client = new KVStore(hostname, port);
            client.addListener(this);
            client.start();
        }

        @Override public KVCommInterface getStore() {

            return (KVCommInterface)client;
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
                    if(tokens.length == 3) {
                        if(client != null && client.isRunning()){
                            
                            StringBuilder msg = new StringBuilder();
                            for(int i = 0; i < tokens.length; i++) {
                                msg.append(tokens[i]);
                                if (i != tokens.length -1 ) {
                                    msg.append(" ");
                                }
                            }
                            sendMessage(msg.toString());

                        } else {
                            printError("Not connected!");
                        }
                    } else {
                        printError("Syntax Error!");
                        printError("put <key> <value>");
                    }
                    break;

                case "get":
                    if(tokens.length == 2) {
                        if(client != null && client.isRunning()){

                            StringBuilder msg = new StringBuilder();
                            for(int i = 0; i < tokens.length; i++) {
                                msg.append(tokens[i]);
                                if (i != tokens.length -1 ) {
                                    msg.append(" ");
                                }
                            }
                            sendMessage(msg.toString());
                            
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

        @Override public void handleNewMessage(TextMessage msg) {
			if (connected) {
                if(!stop) {
                    if (msg.isValid() == 0) {
                        logger.warn("Server Reply Format Invalid. Message: " + msg.getMsg());
                        System.out.println("\rServer Reply Format Invalid.");
                        System.out.println("Expecting: <status> <key> <value> eg: 'GET_SUCCESS key1 test1'");
                        System.out.println("Got: " + msg.getMsg());
                    } else {
                        logger.info("Success: " + msg.getMsg());
                        System.out.println(msg.getMsg());
                    }
                    System.out.print(PROMPT);
                }
			} else {
                logger.info(msg.getMsg());
				
				if (msg.getMsg().indexOf("Connection to storage server established:") != 0) {
					connected = true;
				}

                System.out.println(msg.getMsg());
                System.out.print(PROMPT);
			}
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
