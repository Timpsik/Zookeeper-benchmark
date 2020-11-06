#!/bin/bash

## Check if 3 arguments are given
if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters, 3 expected. Number of clients on node, username and client write rate."
	exit 2
fi

## First argument is the number of Zookeeper clients on each node
numberOfClientsOnNode=$1;

if [ $numberOfClientsOnNode -lt 1 ]; then
	echo "Atleast one client should be on each node. Currently, $numberOfClientsOnNode clients were asked to be created on node."
	exit 2
fi

## Username of the caller
username=$2;

## Write request rate of the Zookeeper clients.
writeRate=$3;

if [ 1 -eq "$(echo "${writeRate} < 0" | bc)" ] || [ 1 -eq "$(echo "${writeRate} > 1" | bc)" ]; then
	echo "Request write rate should be between 0 and 1. Provided: $writeRate"
	exit 2
fi

module load prun

## Get Zookeeper nodes, they are since 9th column, expect them to be the first reservation of the user.
zookeeperNodes=$(preserve -llist | grep $username | sed -n '1p'| awk '{ for (i=9; i<=NF; i++) print $i }')

## Get the current time since Epoch in milliseconds
currentTime=$(date +%s%3N);

## Start the benchmark in 1 minute from now
let startTime=$currentTime+1000*60;

## End the benchmark in 5 minutes from now
let endTime=$currentTime+1000*60*5;

echo "Benchmark Start time: $(date -d @"$((startTime/1000))")";
echo "Benchmark End time: $(date -d @"$((endTime/1000))")";

## From https://stackoverflow.com/a/17841619
function join_by { local d=$1; shift; local f=$1; shift; printf %s "$f" "${@/#/$d}"; }

## Get the Zookeeper cluster address
clusterAddress=$(join_by ":2181," $zookeeperNodes);
clusterAddress+=":2181";

## Reserve nodes for clients
preserve -# 3 -t 00:15:00

## Wait 5 seconds for reservation to show up
sleep 5

## Check if nodes are available to use, if it isn't cancel the reservation.
reservationState=$(preserve -llist | grep $username | sed -n '2p' | awk '{print $7}')

if [ "$reservationState" = "PD" ]; then
	echo " There aren't 3 nodes available to be reserved for clients, try again later."
	scancel -u $username -t "PD"
	exit 2
fi

## Get the client nodes, expect it to be the second reservation for the user
nodes=$(preserve -llist | grep $username | sed -n '2p' | awk '{ for (i=9; i<=NF; i++) print $i }')
clients=$(echo "scale=0 ; $numberOfClientsOnNode * 3" | bc)

echo "Cluster address: $clusterAddress";
echo "Number of total clients: $clients"

## Start the clients in each node
for node in ${nodes}
do
echo "Connecting to node $node"
## Copy the Zookeeper client
## Delete the data from previous run
## Create the directory again
## Start the clients
ssh $node<<-EOF
	if [ ! -d "/local/$username" ]; then
		mkdir "/local/$username"
	fi
	
	cp -r "/var/scratch/$username/Zookeeper/zookeeperClient" "/local/$username/"
	
	rm -rf /var/scratch/$username/Zookeeper/zookeeperClient/$node
	
	mkdir /var/scratch/$username/Zookeeper/zookeeperClient/$node
	
	nohup java -jar /local/$username/zookeeperClient/zookeeper.benchmark.client-1.0.jar $numberOfClientsOnNode $clusterAddress "/${node}_test" $writeRate "/local/$username/zookeeperClient/dummy.bin" $startTime $endTime /local/$username/zookeeperClient &> /var/scratch/$username/Zookeeper/zookeeperClient/$node/output.log &
EOF
done

## Get the current time since epoch
currentTime=$(date +%s%3N);
## Calculate the time until the benchmark is finished, add 7 minutes for the clients to write result into file
let sleep_seconds=($endTime - $currentTime )/1000+420
echo "Going to sleep for $sleep_seconds seconds"
sleep $sleep_seconds

## Collect the results from the clients
for node in ${nodes}
do
echo "Connecting to node $node"

## Get the client request count
## Enter new line
## Get the read request percentage of the client
## Enter new line
ssh $node<<-EOF
	for (( i=0; i < $numberOfClientsOnNode; i++)) 
	do

		head -n 1 "/local/$username/zookeeperClient/client_\${i}_requestCount.txt" >> /var/scratch/$username/Zookeeper/zookeeperClient/$node/result.txt
		
		echo "" >> /var/scratch/$username/Zookeeper/zookeeperClient/$node/result.txt
		
		head -n 1 "/local/$username/zookeeperClient/client_\${i}_readPercentage.txt" >> /var/scratch/$username/Zookeeper/zookeeperClient/$node/reads.txt
		
		echo "" >> /var/scratch/$username/Zookeeper/zookeeperClient/$node/reads.txt
	done
EOF
done

## Read the request counts from file
requestCount=0
for node in ${nodes}
do
	while IFS= read -r line
	do
		requestCount=$(echo "scale=0 ; $requestCount + $line" | bc)
	done < "/var/scratch/$username/Zookeeper/zookeeperClient/$node/result.txt"
done

readPercentage=0;


## Read the read request percentage from file
for node in ${nodes}
do
	while IFS= read -r line
	do
		readPercentage=$(echo "scale=16 ; $readPercentage + $line" | bc)
	done < "/var/scratch/$username/Zookeeper/zookeeperClient/$node/reads.txt"
done

readPercentage=$(echo "scale=25 ; $readPercentage / $clients" | bc)
requestTroughput=$(echo "scale=2 ; $requestCount / 240" | bc)

echo "The test had a read percentage of ${readPercentage}%"
echo "The test had a throughput of ${requestTroughput}"

## Shutdown the Zookeeper cluster
for node in ${zookeeperNodes}
do
ssh $node<<-EOF
	/local/$username/zookeeper/bin/zkServer.sh stop
EOF
done

## Unreserve the nodes 
scancel -u $username