# Zookeeper

This repository contains Zookeeper Java client and 3 shell scripts. Java client is packaged in a jar file. The jar expects 8 arguments for throughput test and 6 arguments for latency test. There are three shell scripts, which are used to run throughput and latency benchmarks inside DAS-5 system.

# Usage

## Java client

Java client is in zookeeperClient folder, called zookeeper.benchmark.client-1.0. It requires Maven to be built from the source. Run "mvn clean install" in the directory. The client jar will be in target directory. One client jar is in zookeeperClient folder. To run the client, the dependency-jars folder needs to be in the same folder as the client.

### Throughput test

It takes 8 command line arguments.
1. Number of clients to create
1. Zookeeper cluster address (host:port,host:port,...)
1. Node path in Zookeeper, each client adds it's own identifier at the end. In case of nested location, the upper directories have to already exist.
1. Request write rate
1. Path to the data, that clients write to Zookeeper.
1. Benchmark start time in milliseconds since epoch.
1. Benchmark end time in milliseconds since epoch.
1. Output directory, benchmark results are written there.

This creates the throughput clients that connect to Zookeeper. Each client makes asynchronous read and write requests to Zookeeper. The distribution between requests is decided based on the input parameter. Client is allowed to have 100 requests waiting for responses before benchmark starts. During the benchmark this is increased to 1000. All clients have own unique path in Zookeeper. After the benchmark it prints out total amount finished request during the benchmark and the read and write request rate.

Example:  

``` java -jar zookeeper.benchmark.client-1.0.jar 20 localhost:2181 /test 0.8 dummy.bin 1604675776000 1604676076000 /local/$username/zookeeperClient```

### Latency test

It takes 6 command line arguments.
1. Number of clients to create
1. Zookeeper cluster address (host:port,host:port,...)
1. Node path in Zookeeper, each client adds it's own identifier at the end. In case of nested location, the upper directories have to already exist.
1. Number of nodes each client should create.
1. Path to the data, that clients write to Zookeeper.
1. Output directory, benchmark results are written there.

This creates the latency clients that connect to Zookeeper. Each client makes synchronous create and asynchronous delete requests to Zookeeper one after another for the given number of nodes asked to be created. All clients have own unique path in Zookeeper. After the benchmark all clients print their start and end time into a file.


Example:  

``` java -jar zookeeper.benchmark.client-1.0.jar 10 localhost:2181 /test 20000 dummy.bin /local/$username/zookeeperClient```

## Bash scripts

### startZookeeperCluster.sh

Script takes three arguments, number of nodes to reserve, time to reserve nodes and username. Time is expected in format hh:mm:ss. This script reserves nodes in DAS-5, creates configuration for Zookeeper cluster and starts the Zookeeper servers. It doesn't shut down the servers. The script checks if there are enough nodes to be reserved and if there isn't, it cancels the reservations for user. **Script expects to be the first reservation for user**.

Example:  
``` ./startZookeeperCluster.sh 3 00:15:00 ddps2005```

### startThroughputClients.sh

Script takes three arguments, number of clients on one node, username and write request rate(from 0 to 1). The script reserves 3 nodes in DAS-5 for clients, starts the Zookeeper clients, waits for them to finish and then collects the results. In the end it stops the Zookeeper servers and **cancels all reservations made by the user**. The script tries to reserve 3 nodes and if the nodes aren't available right away then the reservation is canceled. **Script expects to be second reservation for user and Zookeeper nodes should be first**.

Example:  
``` ./startThroughputClients.sh 50 ddps2005 0.8```

### startLatencyClients.sh

Script takes three arguments, number of clients to use in the test, username and number of nodes each client creates. The script reserves 1 node in DAS-5 for clients. Starts the Zookeeper clients, waits for them to finish and then collects the results. The start and end time are printed out and also put into result.txt file in the folder named after the used node. In the end it stops the Zookeeper servers and **cancels all reservations made by the user**. The script tries to reserve 1 node and if the there aren't any nodes available right away then the reservation is canceled. **Script expects to be second reservation for user and Zookeeper nodes should be first**.

Example:  
``` ./startLatencyClients.sh 10 ddps2005 20000```