package com.dps.zookeeper.client;

import com.dps.zookeeper.connection.ZKLatencyManagerImpl;
import com.dps.zookeeper.connection.ZKManager;
import org.apache.zookeeper.KeeperException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

/**
 * Latency benchmark test client
 */
public class LatencyClient implements ZookeeperClient {

    /**
     * Number of expected arguments for latency benchmark.
     */
    public static final int EXPECTED_ARGUMENTS = 6;

    /**
     * Number of clients in benchmark
     */
    private static final int NUMBER_OF_CLIENTS_IDX = 0;

    /**
     * Zookeeper cluster address
     */
    private static final int CLUSTER_ADDRESS_IDX = 1;

    /**
     * Path in Zookeeper
     */
    private static final int NODE_PATH_IDX = 2;

    /**
     * Number of nodes to create during benchmark
     */
    private static final int NUMBER_OF_NODES_TO_CREATE_IDX = 3;

    /**
     * Location of the data file index.
     */
    private static final int DATA_LOCATION = 4;

    /**
     * Result output directory
     */
    private static final int OUTPUT_DIR_IDX = 5;

    private CountDownLatch countDownLatch;
    private int numberOfClients;
    private String outputDirectory;
    private String clusterAddress;
    private String nodePath;
    private byte[] data;
    private int numberOFNodesToCreate;

    public LatencyClient(String[] args) {
        checkPathInZookeeper(args[NODE_PATH_IDX]);
        parseNumberOfClients(args[NUMBER_OF_CLIENTS_IDX]);
        parseNumberOfNodesToCreate(args[NUMBER_OF_NODES_TO_CREATE_IDX]);
        readData(args[DATA_LOCATION]);
        this.clusterAddress = args[CLUSTER_ADDRESS_IDX];
        this.outputDirectory = args[OUTPUT_DIR_IDX];

    }

    @Override
    public void start() {
        countDownLatch = new CountDownLatch(numberOfClients);
        for (int i = 0; i < numberOfClients; i++) {
            LatencyTask task = new LatencyTask(i, clusterAddress, nodePath, numberOFNodesToCreate, data, outputDirectory);
            Thread t = new Thread(task);
            t.setName("Client_" + i);
            t.start();

        }
    }

    /**
     * Read the used data in the test from file.
     *
     * @param dataPath path to data file
     */
    private void readData(String dataPath) {
        try {
            data = Files.readAllBytes(Paths.get(dataPath));
        } catch (IOException e) {
            throw new IllegalArgumentException("Wasn't able to read data from the provided location: " + dataPath, e);
        }
    }

    /**
     * Check if Zookeeper node path starts with "/".
     *
     * @param zookeeperPath node path in Zookeeper
     */
    private void checkPathInZookeeper(String zookeeperPath) {
        if (!zookeeperPath.startsWith("/")) {
            throw new IllegalArgumentException("Argument at index " + NODE_PATH_IDX +
                    " is expected to with /. Given: " + zookeeperPath +
                    " . It is the path in Zookeeper");
        }
        nodePath = zookeeperPath;
    }

    /**
     * Check if number of clients is positive int.
     *
     * @param numberOfClientsString number of clients in String
     */
    private void parseNumberOfClients(String numberOfClientsString) {
        try {
            numberOfClients = Integer.parseInt(numberOfClientsString);
            if (numberOfClients <= 0) {
                throw new IllegalArgumentException("Argument at index " + NUMBER_OF_CLIENTS_IDX +
                        " is expected to be positive int. Given: " + numberOfClientsString + "" +
                        " . It is the number of clients created.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + NUMBER_OF_CLIENTS_IDX +
                    " is expected to be int. Given: " + numberOfClientsString + "" +
                    " . It is the number of clients created.");
        }
    }

    /**
     * Check if the number of nodes to create is positive int.
     *
     * @param numberOFNodesToCreateString number of nodes to create as String
     */
    private void parseNumberOfNodesToCreate(String numberOFNodesToCreateString) {
        try {
            numberOFNodesToCreate = Integer.parseInt(numberOFNodesToCreateString);
            if (numberOfClients <= 0) {
                throw new IllegalArgumentException("Argument at index " + NUMBER_OF_NODES_TO_CREATE_IDX +
                        " is expected to be positive int. Given: " + numberOFNodesToCreateString + "" +
                        " . It is the number of nodes to be created and deleted in Zookeeper.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + NUMBER_OF_NODES_TO_CREATE_IDX +
                    " is expected to be int. Given: " + numberOFNodesToCreateString + "" +
                    " . It is the number of nodes to be created and deleted in Zookeeper.");
        }
    }

    class LatencyTask implements Runnable {

        private final String clusterAddress;
        private final String path;
        private final int clientId;
        private final byte[] data;
        private final String outputDir;
        private final int numberOfNodes;

        LatencyTask(int clientId, String clusterAddress, String path, int numberOfNodes, byte[] data, String outputDir) {
            this.clientId = clientId;
            this.clusterAddress = clusterAddress;
            this.path = path + clientId;
            this.numberOfNodes = numberOfNodes;
            this.data = data;
            this.outputDir = outputDir;
        }


        @Override
        public void run() {
            ZKLatencyManagerImpl zkManager = null;
            try {
                zkManager = new ZKLatencyManagerImpl(clusterAddress);
                checkPrerequisites(zkManager, path);
                System.out.println("Client_" + clientId + ": starting test");
                long startTime = System.currentTimeMillis();
                for (int i = 0; i < numberOfNodes; i++) {
                    zkManager.create(path, data);
                    zkManager.delete(path);
                }
                long endTime = System.currentTimeMillis();
                countDownLatch.countDown();
                countDownLatch.await();
                writeStart(clientId, startTime);
                writeEnd(clientId, endTime);
                System.out.println("Client_" + clientId + ": Started " + startTime);
                System.out.println("Client_" + clientId + ": Ended " + endTime);
            } catch (Exception e) {
                System.out.println("Client_" + clientId + ": Exception was thrown");
                System.out.println(e.getMessage());
                System.out.println(e.toString());
            } finally {
                if (zkManager != null) {
                    try {
                        zkManager.closeConnection();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Check if the node already exists in Zookeeper and delete it if it does.
         *
         * @param zkManager client
         * @param path      path in Zookeeper
         * @throws InterruptedException
         * @throws KeeperException
         */
        private void checkPrerequisites(ZKManager zkManager, String path) throws InterruptedException, KeeperException {
            if (zkManager.exists(path) != null) {
                zkManager.delete(path);
            }
        }

        /**
         * Write start time of the task
         * @param clientId client identifier
         * @param startTime task start time
         */
        private void writeStart(int clientId, long startTime) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputDir + "/client_" + clientId + "_start.txt"), StandardCharsets.UTF_8)) {
                writer.write(Long.toString(startTime));
                writer.flush();
            } catch (IOException e) {
                System.out.println("Exception when writing requests into file.");
                e.printStackTrace();
            }
        }

        /**
         * Write end  time of the task
         * @param clientId client identifier
         * @param endTime task end time
         */
        private void writeEnd(int clientId, long endTime) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputDir + "/client_" + clientId + "_end.txt"), StandardCharsets.UTF_8)) {
                writer.write(Long.toString(endTime));
                writer.flush();
            } catch (IOException e) {
                System.out.println("Exception when writing requests into file.");
                e.printStackTrace();
            }
        }
    }


}

