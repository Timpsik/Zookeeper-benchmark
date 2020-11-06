package com.dps.zookeeper;

import com.dps.zookeeper.client.LatencyClient;
import com.dps.zookeeper.client.ThroughputClient;
import com.dps.zookeeper.client.ZookeeperClient;

/**
 * Decides which Zookeeper test to run.
 */
public class MainController {

    public static void main(String[] args) {
        // Check if Throughput client or latency client is run.
        if (args.length == ThroughputClient.EXPECTED_ARGUMENTS) {
            ZookeeperClient client = new ThroughputClient(args);
            client.start();
        } else if (args.length == LatencyClient.EXPECTED_ARGUMENTS) {
            ZookeeperClient client = new LatencyClient(args);
            client.start();
        } else {
            System.out.println("Wrong number of arguments given. " +
                    "ThroughputClient expects " + ThroughputClient.EXPECTED_ARGUMENTS +
                    " arguments and LatencyClient expects " + LatencyClient.EXPECTED_ARGUMENTS +
                    " arguments.");
        }
    }
}
