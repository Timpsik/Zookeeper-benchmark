package com.dps.zookeeper.connection;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;

public interface ZKManager {
    /**
     * Creates a node in Zookeeper.
     *
     * @param path path in Zookeeper
     * @param data data to store under node
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String path, byte[] data)
            throws KeeperException, InterruptedException;

    /**
     * Get data stored in the node.
     *
     * @param path      node path in Zookeeper
     * @param watchFlag
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     */
    public void getZNodeData(String path, boolean watchFlag) throws KeeperException, InterruptedException, UnsupportedEncodingException;

    /**
     * Update the data in Zookeeper.
     *
     * @param path node where to store the new data
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void update(String path, byte[] data)
            throws KeeperException, InterruptedException;

    /**
     * Check if node exists in Zookeeper
     *
     * @param path node path in Zookeeper
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat exists(String path) throws KeeperException, InterruptedException;

    /**
     * Delete the node from Zookeeper
     *
     * @param path node to be deleted path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void delete(String path) throws KeeperException, InterruptedException;

    /**
     * Close the Zookeeper connection
     *
     * @throws InterruptedException
     */
    void closeConnection() throws InterruptedException;
}