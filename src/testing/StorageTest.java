package testing;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import app_kvServer.ClientConnection;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;


public class StorageTest extends TestCase {
    private KVServer app;

    @Test
	public void testFIFO() {
        KVServer app = new KVServer(1190,3,"FIFO");
		Exception ex = null;
        new Thread(app).start();
		try {
			app.putKV("0", "1");
            app.putKV("1", "1");
            app.putKV("2", "1");
            app.putKV("3", "1");
		} catch (Exception e) {
		    ex = e;	
		}
		assertTrue(app.inStorage("0") && !app.inCache("0"));
	}

    @Test
	public void testLRU() {
        KVServer app = new KVServer(1190,3,"LRU");
		Exception ex = null;
        new Thread(app).start();
		try {
			app.putKV("0", "1");
            app.putKV("1", "1");
            app.putKV("0", "1");
            app.putKV("2", "1");
            app.putKV("3", "1");
		} catch (Exception e) {
		    ex = e;	
		}
		assertTrue(app.inStorage("1") && !app.inCache("1"));
	}

    @Test
	public void testLFU() {
        KVServer app = new KVServer(1190,3,"LFU");
		Exception ex = null;
        new Thread(app).start();
		try {
			app.putKV("0", "1");
            app.putKV("0", "1");
            app.putKV("0", "1");
            app.putKV("2", "1");
            app.putKV("2", "1");
            app.putKV("3", "1");
            app.putKV("4", "1");
		} catch (Exception e) {
		    ex = e;	
		}
		assertTrue(app.inStorage("3") && !app.inCache("3"));
	}



}



