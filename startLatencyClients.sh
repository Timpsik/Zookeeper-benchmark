#!/bin/bash

## Check if 3 arguments are given
if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters, 3 expected. Number of clients on node, username and how many nodes each client should create."
	exit 2
fi

## Number of clients
numberOfWorkers=$1;

if [ $numberOfWorkers -lt 1 ]; then
	echo "Atleast one worker should be created. Currently, $numberOfWorkers workers were asked to be created."
	exit 2
fi

## Username of the caller
username=$2;

## number of nodes each client creates
numberOfCreatedNodes=$3;

if [ $numberOfCreatedNodes -lt 1 ]; then
	echo "Invalid number of created nodes. Expected more than 1, you gave: $numberOfCreatedNodes"
	exit 2
fi


module load prun

## Get zookeeper nodes
zookeeperNodes=$(preserve -llist | grep $username | sed -n '1p'| awk '{ for (i=9; i<=NF; i++) print $i }')


## From https://stackoverflow.com/a/17841619
function join_by { local d=$1; shift; local f=$1; shift; printf %s "$f" "${@/#/$d}"; }


clusterAddress=$(join_by ":2181," $zookeeperNodes);
clusterAddress+=":2181";

## Reserve one node for clients
preserve -# 1 -t 00:15:00
sleep 5

reservationState=$(preserve -llist | grep $username | sed -n '2p'  | awk '{print $7}')

## Check if nodes are available to use, if it isn't cancel the reservation.
if [ "$reservationState" = "PD" ]; then
	echo " There isn't node available to be reserved for clients, try again later."
	scancel -u $username -t "PD"
	exit 2
fi

## Get the client node
clientNode=$(preserve -llist | grep $username | sed -n '2p' | awk '{ for (i=9; i<=NF; i++) print $i }')

echo "Cluster address: $clusterAddress";

## Run the benchmark and find the first start and last end
echo "Connecting to node $clientNode"
ssh $clientNode<<-EOF
	if [ ! -d "/local/$username" ]; then
		mkdir "/local/$username"

	fi
	cp -r "/var/scratch/$username/Zookeeper/zookeeperClient" "/local/$username/"
	rm -r /var/scratch/$username/Zookeeper/zookeeperClient/$clientNode
	mkdir /var/scratch/$username/Zookeeper/zookeeperClient/$clientNode
	echo "Starting clients"
	java -jar /local/$username/zookeeperClient/zookeeper.benchmark.client-1.0.jar $numberOfWorkers $clusterAddress "/test"  $numberOfCreatedNodes /local/$username/zookeeperClient/dummy.bin /local/$username/zookeeperClient 
	echo "Clients finished"

	first=true
	startTime=0
	endTime=0
	for (( i=0; i < $numberOfWorkers; i++)) 
	do
		workerStartTime=\$(head -n 1 "/local/$username/zookeeperClient/client_\${i}_start.txt") 
		workerEndTime=\$(head -n 1 "/local/$username/zookeeperClient/client_\${i}_end.txt") 
		if [ "\$first" = true ] ; then
			startTime=\$workerStartTime;
			endTime=\$workerEndTime;
			first=false
		elif [ \$workerStartTime -lt \$startTime ] ; then
			startTime=\$workerStartTime;
		fi

		if [ \$workerEndTime -gt \$endTime ] ; then
			endTime=\$workerEndTime;
		fi
	done
	echo "\$startTime" >> /var/scratch/$username/Zookeeper/zookeeperClient/$clientNode/result.txt
	echo "\$endTime" >> /var/scratch/$username/Zookeeper/zookeeperClient/$clientNode/result.txt
EOF

## Read the start and end result
while IFS= read -r line
do
	echo "$line"
done < "/var/scratch/$username/Zookeeper/zookeeperClient/$clientNode/result.txt"


## Shut down Zookeeper cluster
for node in ${zookeeperNodes}
do
ssh $node<<-EOF
 /local/$username/zookeeper/bin/zkServer.sh stop
EOF
done

## Free the nodes 
scancel -u $username