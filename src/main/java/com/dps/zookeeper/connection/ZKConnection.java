package com.dps.zookeeper.connection;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Zookeeper connection initializer.
 */
public class ZKConnection {
    /**
     * Zookeeper client.
     */
    private ZooKeeper zoo;

    /**
     * CountDown for waiting the connection to be made
     */
    CountDownLatch connectionLatch = new CountDownLatch(1);

    /**
     * Create the connection to Zookeeper
     *
     * @param host cluster address
     * @return client
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper connect(String host) throws IOException, InterruptedException {
        zoo = new ZooKeeper(host, 20000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        });
        // Wait for the connection to be made.
        connectionLatch.await();
        return zoo;
    }

    /**
     * Close the client connection
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zoo.close();
    }
}