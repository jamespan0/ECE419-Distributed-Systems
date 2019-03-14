package app_kvServer;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.KVAdminMessage;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ECSConnection implements Runnable {

	KVServer server;

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket ecsSocket;
	private InputStream input;
	private OutputStream output;
	
	/**
	 * Constructs a new ClientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ECSConnection(KVServer server, Socket ecsSocket) {
		this.server = server;
		this.ecsSocket = ecsSocket;
		this.isOpen = true;
	}

	public void parseCommand(KVAdminMessage message) {
		String[] stringArray = message.getMsg().split("\\s+");

		switch (stringArray[0]) {
			case ("INIT"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + " " + stringArray[1] + " " + stringArray[2] + "...'");

					String[] tempArray = message.getMsg().split(" ", 4);

					server.initKVServer(tempArray[3], Integer.parseInt(tempArray[1]), tempArray[2]);
					
					sendMessage(new KVAdminMessage("INIT_SUCCESS"));
					
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;

			case ("START"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");

					server.start();
					
					sendMessage(new KVAdminMessage("START_SUCCESS"));
					
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;

			case ("STOP"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");

					server.stop();
					
					sendMessage(new KVAdminMessage("STOP_SUCCESS"));
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;

			case ("LOCK"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");

					server.lockWrite();
					
					sendMessage(new KVAdminMessage("LOCK_SUCCESS"));
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;

			case ("UNLOCK"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");

					server.unLockWrite();
					
					sendMessage(new KVAdminMessage("UNLOCK_SUCCESS"));
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;
			

			case ("SHUT_DOWN"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");

					server.unLockWrite();
					
					sendMessage(new KVAdminMessage("SHUTDOWN_SUCCESS"));
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
				}

				break;
			

			case ("UPDATE"):
				try {
					logger.info("ECS Message \t<" 
							+ ecsSocket.getInetAddress().getHostAddress() + ":" 
							+ ecsSocket.getPort() + ">: 'ECS: " 
							+ stringArray[0] + "'");
					
					sendMessage(new KVAdminMessage("UPDATE_SUCCESS"));
				} catch (Exception e) {

					logger.error("Error: ECS command unsuccessful!", e);
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
			output = ecsSocket.getOutputStream();
			input = ecsSocket.getInputStream();
		
			sendMessage(new KVAdminMessage(
					"KV server started on: " 
					+ server.getHostname() + ":"
					+ server.getPort()));
			
			while(isOpen) {
				try {
					KVAdminMessage latestMsg = receiveMessage();
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
	 * Method sends a KVAdminMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVAdminMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ ecsSocket.getInetAddress().getHostAddress() + ":" 
				+ ecsSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
        }
	
	
	private KVAdminMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
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
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
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
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		KVAdminMessage msg = new KVAdminMessage(msgBytes);
		logger.info("RECEIVE \t<" 
				+ ecsSocket.getInetAddress().getHostAddress() + ":" 
				+ ecsSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");

		return msg;
    }
	
        private void closeConnection() {
            isOpen = false;
            try {
                if (ecsSocket != null) {
                    input.close();
                    output.close();
                    ecsSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
	
}
