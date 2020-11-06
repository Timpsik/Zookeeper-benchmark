package com.dps.zookeeper.connection;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proxy for Zookeeper client methods for throughput benchmark test.
 */
public class ZKThroughputManagerImpl implements ZKManager {

    /**
     * Zookeeper client
     */
    private ZooKeeper zkeeper;

    /**
     * Zookeeper connection
     */
    private ZKConnection zkConnection;

    /**
     * Node data version.
     */
    private int version = 0;

    /**
     * Total amount of requests successfully finished during the benchmark.
     */
    public AtomicLong requestsDone = new AtomicLong(0);

    /**
     * Total amount of write requests successfully finished during the benchmark.
     */
    public AtomicLong writeRequestsDone = new AtomicLong(0);

    /**
     * Total amount of read requests successfully finished during the benchmark.
     */
    public AtomicLong readRequestsDone = new AtomicLong(0);

    /**
     * Flag for deciding if the benchmark has started and request should be counted.
     */
    private boolean shouldCountRequests = false;

    /**
     * Allow client to have 100 requests waiting in case benchmark hasn't started yet.
     */
    private AtomicInteger startUpRequests = new AtomicInteger(100);

    /**
     * Allow client to have 1K requests waiting if benchmark has started.
     * Otherwise server will be overwhelmed and might close the client connection since it is busy with older requests.
     */
    private AtomicInteger allowedRequests = new AtomicInteger(1000);


    public ZKThroughputManagerImpl(String serverAddress) throws IOException, InterruptedException {
        initialize(serverAddress);
    }

    /**
     * Creates ephemeral node in Zookeeper. The node is automatically deleted after client disconnects.
     *
     * @param path path in Zookeeper
     * @param data data to store under node
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    public void create(String path, byte[] data)
            throws KeeperException,
            InterruptedException {
        zkeeper.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
    }

    /**
     * Get the data stored in the node asynchronously.
     *
     * @param path      node path in Zookeeper
     * @param watchFlag
     */
    @Override
    public void getZNodeData(String path, boolean watchFlag) {
        // Check if benchmark has started.
        if (!shouldCountRequests) {
            zkeeper.getData(path, false, (((rc, path1, ctx, data, stat) -> {
                startUpRequests.incrementAndGet();
            })), null);
        } else {
            zkeeper.getData(path, false, (((rc, path1, ctx, data, stat) -> {
                allowedRequests.incrementAndGet();
                if (rc == KeeperException.Code.OK.intValue()) {
                    readRequestsDone.incrementAndGet();
                    requestsDone.incrementAndGet();
                }
            })), null);

        }
    }

    /**
     * Update the data stored in the node asynchronously.
     *
     * @param path node where to store the new data
     * @param data
     */
    @Override
    public void update(String path, byte[] data) {
        // Check if benchmark has started.
        if (!shouldCountRequests) {
            zkeeper.setData(path, data, version, ((rc, path1, ctx, stat) -> {
                startUpRequests.incrementAndGet();
            }), null);

        } else {

            zkeeper.setData(path, data, version, ((rc, path1, ctx, stat) -> {
                allowedRequests.incrementAndGet();

                if (rc == KeeperException.Code.OK.intValue()) {
                    writeRequestsDone.incrementAndGet();
                    requestsDone.incrementAndGet();
                }
            }), null);
        }
        // Increase the data version.
        version++;
    }

    @Override
    public Stat exists(String path) throws KeeperException, InterruptedException {
        return zkeeper.exists(path, false);
    }

    @Override
    public void delete(String path) throws KeeperException, InterruptedException {
        zkeeper.delete(path, 0, null, null);
    }

    /**
     * Start the benchmark and request counting.
     */
    public void startRequestCounting() {
        shouldCountRequests = true;
    }

    /**
     * Check if the client can make a request.
     * During benchmark client can have 10K requests, before only 100 requests.
     *
     * @return true, if client can make a request.
     */
    public boolean allowedToMakeRequest() {
        if (shouldCountRequests) {
            // If benchmark has started, check if the 1K requests have been made
            return allowedRequests.get() > 0;
        } else {
            // Benchmark hasn't started, check if the 100 requests have been made
            return startUpRequests.get() > 0;
        }
    }

    /**
     * Reduce the amount of available requests.
     */
    public void reduceAllowedRequestCount() {
        if (shouldCountRequests) {
            // During benchmark reduce 1K count
            allowedRequests.decrementAndGet();
        } else {
            // Before benchmark reduce 100 count
            startUpRequests.decrementAndGet();
        }
    }

    /**
     * Create the Zookeeper connection
     *
     * @param serverAddress Zookeeper cluster address
     * @throws IOException
     * @throws InterruptedException
     */
    private void initialize(String serverAddress) throws IOException, InterruptedException {
        zkConnection = new ZKConnection();
        zkeeper = zkConnection.connect(serverAddress);
    }

    /**
     * Close the Zookeeper connection.
     *
     * @throws InterruptedException
     */
    public void closeConnection() throws InterruptedException {
        zkConnection.close();
    }
}