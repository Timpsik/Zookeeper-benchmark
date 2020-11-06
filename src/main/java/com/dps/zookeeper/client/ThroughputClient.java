package com.dps.zookeeper.client;

import com.dps.zookeeper.connection.ZKManager;
import com.dps.zookeeper.connection.ZKThroughputManagerImpl;
import org.apache.zookeeper.KeeperException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Tests the throughput of the Zookeeper cluster.
 */
public class ThroughputClient implements ZookeeperClient {

    /**
     * Number of expected command line arguments
     */
    public static final int EXPECTED_ARGUMENTS = 8;

    /**
     * Number of clients, which are created.
     */
    private static final int NUMBER_OF_CLIENTS_IDX = 0;

    /**
     * Zookeeper cluster address, "serverIp1:port,serverIp2:port,...."
     */
    private static final int CLUSTER_ADDRESS_IDX = 1;

    /**
     * Path in Zookeeper. Each client adds it's ID to the end of path name.
     * In case of multiple nodes used in test, the given path should be different on each node.
     */
    private static final int NODE_PATH_IDX = 2;

    /**
     * Percentage of writes in generated requests
     */
    private static final int WRITE_RATE_IDX = 3;

    /**
     * Path to data, which is used in the test
     */
    private static final int DATA_PATH_IDX = 4;

    /**
     * Start time of the benchmark test. Should be in future to ensure all clients are ready.
     */
    private static final int START_TIME_IDX = 5;

    /**
     * End time of the benchmark test.
     */
    private static final int END_TIME_IDX = 6;

    /**
     * Directory where to write the benchmark results.
     */
    private static final int OUTPUT_DIR_IDX = 7;

    /**
     * Used to wait for all clients to finish request generation and  then start writing results into file.
     */
    private static CountDownLatch threadCountDown;


    private int numberOfClients;
    private String clusterAddress;
    private String pathInZookeeper;
    private double writeRequestRate;
    private byte[] data;
    private long benchmarkStartTime;
    private long benchmarkEndTime;
    private String outputDirectory;

    public ThroughputClient(String[] args) {
        validateAndGetInputs(args);
        this.clusterAddress = args[CLUSTER_ADDRESS_IDX];
        this.outputDirectory = args[OUTPUT_DIR_IDX];
    }


    @Override
    public void start() {
        threadCountDown = new CountDownLatch(numberOfClients);
        for (int i = 0; i < numberOfClients; i++) {
            ThroughputTask tasl = new ThroughputTask(i, clusterAddress, pathInZookeeper,
                    writeRequestRate, data,
                    benchmarkStartTime, benchmarkEndTime, outputDirectory);
            Thread t = new Thread(tasl);
            t.setName("Client_" + i);
            t.start();
        }
    }


    /**
     * Check if the given inputs are valid
     *
     * @param args command line arguments
     */
    private void validateAndGetInputs(String[] args) {
        parseNumberOfClients(args[NUMBER_OF_CLIENTS_IDX]);
        checkPathInZookeeper(args[NODE_PATH_IDX]);
        parseWriteRate(args[WRITE_RATE_IDX]);
        readData(args[DATA_PATH_IDX]);
        parseAndCheckStartTime(args[START_TIME_IDX]);
        parseAndCheckEndTime(args[END_TIME_IDX]);
    }

    /**
     * Check if benchmark end time is long and after start time and in the future.
     *
     * @param endTimeString Benchmark end time in String
     */
    private void parseAndCheckEndTime(String endTimeString) {
        try {
            benchmarkEndTime = Long.parseLong(endTimeString);
            if (benchmarkEndTime < benchmarkStartTime) {
                throw new IllegalArgumentException("Benchmark end time is before benchmark start time. " +
                        "Start: " + benchmarkStartTime + ", End: " + benchmarkEndTime);
            } else if (benchmarkEndTime < System.currentTimeMillis()) {
                throw new IllegalArgumentException("Benchmark end time is in the past " +
                        "Current time: " + System.currentTimeMillis() + ", End: " + benchmarkEndTime);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + END_TIME_IDX +
                    " is expected to be long. Given: " + endTimeString +
                    " . It is the start time of benchmark.");
        }
    }

    /**
     * Check if the start time is long and start is in the future,
     * otherwise it isn't possible to calculate correct throughput.
     *
     * @param startTimeString Benchmark start time in String
     */
    private void parseAndCheckStartTime(String startTimeString) {
        try {
            benchmarkStartTime = Long.parseLong(startTimeString);
            if (System.currentTimeMillis() > benchmarkStartTime) {
                throw new IllegalArgumentException("Benchmark start time is in the past " +
                        "Current time: " + System.currentTimeMillis() + ", Start: " + benchmarkStartTime);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + START_TIME_IDX +
                    " is expected to be long. Given: " + startTimeString +
                    " . It is the start time of benchmark.");
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
     * Check if request write rate is double and between 1 and 0.
     *
     * @param writeRateString request write rate in String
     */
    private void parseWriteRate(String writeRateString) {
        try {
            writeRequestRate = Double.parseDouble(writeRateString);
            if (writeRequestRate > 1 || writeRequestRate < 0) {
                throw new IllegalArgumentException("Argument at index " + WRITE_RATE_IDX +
                        " is expected to be double between 0 and 1. Given: " + writeRateString +
                        " . It is the write request generation rate.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + WRITE_RATE_IDX +
                    " is expected to be double. Given: " + writeRateString +
                    " . It is the write request generation rate.");
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
        pathInZookeeper = zookeeperPath;
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

    class ThroughputTask implements Runnable {

        private final String clusterAddress;
        private final String path;
        private final byte[] data;
        private final double writeRate;
        private boolean started = false;
        private final long startTime;
        private final long endTime;
        private final int clientId;
        private final String outputDir;

        ThroughputTask(int clientId, String clusterAddress, String path, double writeRate, byte[] data,
                       long startTime, long endTime, String outputDir) {
            this.clientId = clientId;
            this.clusterAddress = clusterAddress;
            this.path = path + clientId;
            this.writeRate = writeRate;
            this.data = data;
            this.startTime = startTime;
            this.endTime = endTime;
            this.outputDir = outputDir;
        }


        @Override
        public void run() {
            ZKThroughputManagerImpl zkManager = null;
            try {
                zkManager = new ZKThroughputManagerImpl(clusterAddress);
                Random random = new Random();
                // Create path in Zookeeper if it doesn't exist
                checkPrerequisites(zkManager, path, data);
                double writeRequests;
                double readRequests;
                long numberOfRequests;

                // Start request generation
                while (true) {
                    if (!started && System.currentTimeMillis() > startTime) {
                        // Start the benchmark (request counting)
                        started = true;
                        zkManager.startRequestCounting();
                    }

                    // Check if client can make additional requests
                    if (zkManager.allowedToMakeRequest()) {
                        zkManager.reduceAllowedRequestCount();
                        if (random.nextDouble() <= writeRate) {
                            zkManager.update(path, data);
                        } else {
                            zkManager.getZNodeData(path, false);
                        }
                    }

                    // Check if the benchmark is finished
                    if (System.currentTimeMillis() > endTime) {
                        // Get the number of made requests.
                        numberOfRequests = zkManager.requestsDone.get();
                        writeRequests = zkManager.writeRequestsDone.doubleValue();
                        readRequests = zkManager.readRequestsDone.doubleValue();

                        // reduce the countdown
                        threadCountDown.countDown();

                        break;
                    }
                }
                // Wait for all threads to finish, before doing any I/O
                threadCountDown.await();

                System.out.println("Client_" + clientId + ": Did " + numberOfRequests + " requests");
                System.out.println("Client_" + clientId + ": Did " + writeRequests + " write requests, it is " + writeRequests / numberOfRequests * 100 + "%");
                System.out.println("Client_" + clientId + ": Did " + readRequests + " read requests, it is " + readRequests / numberOfRequests * 100 + "%");

                // Write results into a file
                writeCount(clientId, numberOfRequests);
                writeReadPercentage(clientId, readRequests / numberOfRequests * 100);
            } catch (Exception e) {
                System.out.println("Client_" + clientId + ": Exception was thrown");
                System.out.println(e.getMessage());
                System.out.println(e.toString());
            } finally {
                // Close zookeeper connection
                if (zkManager != null) {
                    try {
                        zkManager.closeConnection();
                    } catch (InterruptedException e) {
                        System.out.println("Exception when trying to close Zookeeper: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Create the node for the client, if it doesn't exist in Zookeeper
         *
         * @param zkManager Zookeeper connection
         * @param path      node path in Zookeeper
         * @param data      data, which is written to node
         * @throws InterruptedException
         * @throws KeeperException
         */
        private void checkPrerequisites(ZKManager zkManager, String path, byte[] data) throws InterruptedException, KeeperException {
            if (zkManager.exists(path) == null) {
                zkManager.create(path, data);
            }
        }

        /**
         * Write the percentage of made read requests into a file
         *
         * @param clientId     client identifier
         * @param readRequests percentage of read requests
         */
        private void writeReadPercentage(int clientId, double readRequests) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputDir + "/client_" + clientId + "_readPercentage.txt"), StandardCharsets.UTF_8)) {
                writer.write(Double.toString(readRequests));
                writer.flush();
            } catch (IOException e) {
                System.out.println("Exception when writing requests into file.");
                e.printStackTrace();
            }
        }

        /**
         * Write total amount of requests made into a file
         *
         * @param clientId        client identifier
         * @param numberOfRequest number of requests made
         */
        private void writeCount(int clientId, long numberOfRequest) {
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(outputDir + "/client_" + clientId + "_requestCount.txt"), StandardCharsets.UTF_8)) {
                writer.write(Long.toString(numberOfRequest));
                writer.flush();
            } catch (IOException e) {
                System.out.println("Exception when writing requests into file.");
                e.printStackTrace();
            }
        }
    }

}
