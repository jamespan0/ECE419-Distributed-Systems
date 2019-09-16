package app_kvECS;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import java.nio.ByteBuffer;

import java.io.*;

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

import org.json.*;

public class ECSServerMonitor implements Runnable, Watcher {
    private static Logger logger = Logger.getRootLogger();

    private ECSClient ecs;

    private static final String ZK_CONNECT = "127.0.0.1:2181";
    private static final int ZK_TIMEOUT = 2000;

    public boolean running = false;

    private ZooKeeper zk;
    private CountDownLatch connectedSignal;

    public Set<String> testActive = new TreeSet<String>();
    public Set<String> needResponse = new TreeSet<String>();

    public ECSServerMonitor(ECSClient ecs) {
        this.ecs = ecs;
    }

    public void run() {
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

            //heartbeat every 30s
            int heartbeat = 30000;

            while (running) {
                long currentTime = System.currentTimeMillis();

                for (String failed : needResponse) {
                    ecs.replace(failed);
                    needResponse.remove(failed);
                }


                if (running) {

                    for (String server : testActive) {
                        Stat st = zk.exists("/" + server + "/heartbeat", this);
                        if (st == null) {
                            zk.create("/" + server + "/heartbeat", "".getBytes(),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            st = zk.exists("/" + server + "/heartbeat", this);
                        } else {
                            zk.setData("/" + server + "/message", "HEARTBEAT".getBytes(), zk.exists("/" + server + "/message", false).getVersion());

                            needResponse.add(server);
                        }
                    }
                }

                //wait heartbeat time
                while (System.currentTimeMillis() - currentTime < heartbeat && running) {
                }
            }
        } catch (Exception e) {
            logger.error(e);
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent we) {
        String server = we.getPath().split("/")[1];
        needResponse.remove(server);
    }
}
