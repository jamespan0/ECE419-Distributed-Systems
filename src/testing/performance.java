package testing;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import app_kvServer.ClientConnection;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.util.Random;

public class performance extends TestCase {
    private KVStore kvClient_FIFO;
    private KVStore kvClient_LRU;
    private KVStore kvClient_LFU;
    private KVServer perf_server_FIFO;
    private KVServer perf_server_LRU;
    private KVServer perf_server_LFU;
    private int port;

    public performance() {

        perf_server_FIFO = new KVServer(1190,3,"FIFO");
        perf_server_LRU = new KVServer(1191,3,"LRU");
        perf_server_LFU = new KVServer(1191,3,"LFU");

		Exception ex = null;
//        new Thread(perf_server_FIFO).start();
//        new Thread(perf_server_LRU).start();
        new Thread(perf_server_LFU).start();

		try {

            Random rg = new Random(3);

            for (int i = 0; i < 400; i++) {
                if (rg.nextInt(30) %5 == 0) {
                    perf_server_LFU.getKV("key" + rg.nextInt(100));
                } else {
                    perf_server_LFU.putKV("key" + rg.nextInt(100), "value1");
                }
            }

            perf_server_LFU.close();


		} catch (Exception e) {
		    ex = e;	
		}

        System.out.println("END");
    }

}
