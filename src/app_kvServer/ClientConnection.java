package app_kvServer;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.*;
import org.json.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import shared.messages.TextMessage;

import client.KVCommInterface;
import client.KVStore;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    KVServer server;

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;


    /**
     * Constructs a new ClientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(KVServer server, Socket clientSocket) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    public void parseCommand(TextMessage message) {
        String[] stringArray = message.getMsg().split("\\s+");

        switch (stringArray[0]) {
            case ("put"):
                try {
                    if (server.activated) {
                        logger.info("PUT_KV \t<"
                                + clientSocket.getInetAddress().getHostAddress() + ":"
                                + clientSocket.getPort() + ">: 'PUT<"
                                + stringArray[1] + "," + stringArray[2] + ">'");
                        if (server.writeLock) {
                            sendMessage(new TextMessage("SERVER_WRITE_LOCK"));
                        } else {
                            String result;
                            String key = server.hashing(stringArray[1]);

                            if (!server.checkResponsibility(key)) {
                                sendMessage(new TextMessage("SERVER_NOT_RESPONSIBLE " + server.JSONmetaData.toString()));
                            } else {
                                synchronized (server) {
                                    result = server.putKV(key, stringArray[2]);
                                }

                                sendMessage(new TextMessage("PUT_" + result + " " + stringArray[1] + " " + stringArray[2]));
                            }
                        }
                    } else {
                        sendMessage(new TextMessage("SERVER_STOPPED"));
                    }

                } catch (Exception e) {
                    logger.error("Error: PUT command unsuccessful!", e);
                }

                break;

            case ("get"):
                try {
                    if (server.activated) {
                        logger.info("GET_KV \t<"
                                + clientSocket.getInetAddress().getHostAddress() + ":"
                                + clientSocket.getPort() + ">: 'GET<"
                                + stringArray[1] + ">'");

                        String value;

                        String key = server.hashing(stringArray[1]);

                        if (!server.checkGetResposibility(key)) {
                            sendMessage(new TextMessage("SERVER_NOT_RESPONSIBLE " + server.JSONmetaData.toString()));
                        } else {
                            synchronized (server) {
                                value = server.getKV(key);
                            }

                            sendMessage(new TextMessage("GET_SUCCESS " + stringArray[1] + " " + value));
                        }
                    } else {
                        sendMessage(new TextMessage("SERVER_STOPPED"));
                    }

                } catch (Exception e) {
                    logger.error("Error: GET command unsuccessful!", e);
                }

                break;
            case "MOVE_DATA":
                try {
                    JSONArray newData = new JSONArray(stringArray[1]);
                    for (int i = 0; i < newData.length(); i++) {
                        JSONObject dataNode = newData.getJSONObject(i);

                        String key = dataNode.getString("key");
                        String value = dataNode.getString("value");
                        synchronized (server) {
                            value = server.putKVNoReplicate(key, value);
                        }
                    }

                    for (String replica : server.replicas) {
                        if (!replica.equals("")) {
                            KVStore client = new KVStore(server.metaData.get(replica).getNodeHost(), server.metaData.get(replica).getNodePort());
                            client.connect();
                            client.addListener(server);
                            client.start();

                            TextMessage msg = new TextMessage("REPLICATE " + newData.toString());

                            int timeout = 5000;
                            long currentTime = System.currentTimeMillis();

                            while (System.currentTimeMillis() - currentTime < timeout && !server.transferConnected) {

                            }

                            server.transferConnected = false;

                            client.sendMessage(msg);
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    e.printStackTrace();
                }
                break;

            case "REPLICATE":
                JSONArray replicatedData = new JSONArray(stringArray[1]);
                for (int i = 0; i < replicatedData.length(); i++) {
                    JSONObject dataNode = replicatedData.getJSONObject(i);

                    String key = dataNode.getString("key");
                    String value = dataNode.getString("value");
                    try {
                        synchronized (server) {
                            value = server.putKVNoReplicate(key, value);
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                }
                break;

            default:
                logger.error("Error: Unknown Command!");
                break;
        }
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            sendMessage(new TextMessage(
                    "CONNECTION_SUCCESS " + server.JSONmetaData.toString()));

            while (isOpen) {
                try {
                    TextMessage latestMsg = receiveMessage();
                    parseCommand(latestMsg);
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        } finally {

            closeConnection();
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     *
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() + "'");
    }


    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        // Check for TCP FIN
        if (read == -1) {
            // Close connection
            closeConnection();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");

        return msg;
    }

    private void closeConnection() {
        isOpen = false;
        try {
            if (clientSocket != null) {
                input.close();
                output.close();
                clientSocket.close();
            }
        } catch (IOException ioe) {
            logger.error("Error! Unable to tear down connection!", ioe);
        }
    }

}
