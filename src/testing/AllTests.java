package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
                        new LogSetup("logs/testing/test.log", Level.ALL);
                        KVServer svr = new KVServer(50000, 10, "FIFO");
                        Thread svr_thread = new Thread(svr);
                        svr_thread.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
//		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class); 
//		clientSuite.addTestSuite(AdditionalTest.class); 
		clientSuite.addTestSuite(StorageTest.class); 
		return clientSuite;
	}
	
}
