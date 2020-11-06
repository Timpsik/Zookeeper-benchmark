package com.dps.zookeeper.connection;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class ZKLatencyManagerImpl implements ZKManager {
    private ZooKeeper client;
    private ZKConnection zkConnection;
    private int version;

    public ZKLatencyManagerImpl(String clusterAddress) throws IOException, InterruptedException {
        initialize(clusterAddress);
    }

    private void initialize(String clusterAddress) throws IOException, InterruptedException {
        zkConnection = new ZKConnection();
        client = zkConnection.connect(clusterAddress);
    }

    /**
     * Synchronous create of the node.
     * @param path path in Zookeeper
     * @param data data to store under node
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    public void create(String path, byte[] data) throws KeeperException, InterruptedException {
        client.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    @Override
    public void getZNodeData(String path, boolean watchFlag) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        client.getData(path, watchFlag, null);
    }

    @Override
    public void update(String path, byte[] data) throws KeeperException, InterruptedException {
        client.setData(path, data, version);
        version++;
    }

    @Override
    public Stat exists(String path) throws KeeperException, InterruptedException {
        return client.exists(path, false);
    }

    /**
     * Asynchronous delete of the node
     * @param path node to be deleted path
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    public void delete(String path) throws KeeperException, InterruptedException {
    client.delete(path,version,((rc, path1, ctx) -> {

    }),null);
    }

    @Override
    public void closeConnection() throws InterruptedException {
        zkConnection.close();
    }
}
