#!/bin/bash

## Check if 3 arguments are given
if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters, 3 expected. Number of servers, time to reserve nodes and username"
	exit 2
fi

## Number of Zookeeper servers
numberOfServers=$1;
if [ $numberOfServers -lt 1 ]; then
	echo "Atleast one node should be requested for Zookeeper cluster. Currently, you asked the cluster size to be $numberOfServers."
	exit 2
fi
echo "Number of nodes requested: $1";

## Time to reserve the nodes, hh:mm:ss
timeToReserve=$2;

## Username of the caller
username=$3;

## Zookeeper 3.6.2 download location
zooKeeperDownloadLocation="https://apache.newfountain.nl/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz"

module load prun

## Reserve the nodes
preserve -# $numberOfServers -t $timeToReserve

## Wait for reservation to finalise
sleep 5

reservationState=$(preserve -llist | grep $username | awk '{print $7}')

echo "Reservation state: $reservationState"

if [ "$reservationState" = "PD" ]; then
	echo " There aren't $numberOfServers nodes available to be reserved, try again later"
	scancel -u $username
	exit 2
fi

## Get the reserved nodes, expect that there aren't any other reservations for the user and nodes are available
nodes=$(preserve -llist | grep $username | awk '{ for (i=9; i<=NF; i++) print $i }')

## Create array of the nodes
nodeNumber=0;
for node in ${nodes}
do
	nodeArray[$nodeNumber]=$node;
	nodeNumber=$nodeNumber+1;
done

## Check if Zookeeper binaries exist in DAS-5 cluster site
if [ ! -d "/var/scratch/$username/zookeeper" ]
then
   echo "Zookeeper wasn't found in scratch, downloading it..."
   ## Download the Zookeeper binaries in one of the nodes
   echo "Establishing connection to ${nodeArray[0]}"
ssh ${nodeArray[0]} <<-EOF
    (cd "/var/scratch/${username}";wget "$zooKeeperDownloadLocation";
    mkdir  zookeeper;
    tar -xf "apache-zookeeper-3.6.2-bin.tar.gz" -C zookeeper --strip-components=1;
    )
EOF

else
   echo "Zookeeper binaries already exist"
fi



## Function for Zookeeper conf file creation
createConfFile(){
	numberOfservers=$1;
	username=$2;
	shift 2
	nodeArray=("$@")
	fileLoc="/local/$username/zookeeper/conf/zoo.cfg"
	echo "tickTime=2000" >> $fileLoc;
	echo "dataDir=/local/$username/zookeeper/data" >> $fileLoc;
	echo "clientPort=2181" >> $fileLoc;
	echo "globalOutstandingLimit=2000" >> $fileLoc;
	echo "initLimit=10" >> $fileLoc;
	echo "syncLimit=5" >> $fileLoc;
	echo "admin.serverPort=3245" >> $fileLoc;
	for (( j=1; j <= $numberOfservers; j++))
	do
		echo "server.${j}=${nodeArray[$j-1]}:2888:3888" >> $fileLoc;
	done
}



## Create Zookeeper configurations
for (( i=1; i <= $numberOfServers; i++))
do
	echo "Establishing connection to ${nodeArray[$i-1]}"
ssh ${nodeArray[$i-1]} <<-EOF
	if [ ! -d "/local/$username" ]; then
		mkdir "/local/$username"
	else
		rm -r "/local/$username"
		mkdir "/local/$username"
	fi
	mkdir "/local/$username/zookeeper"
	cp -r "/var/scratch/$username/zookeeper" "/local/$username"
	mkdir "/local/$username/zookeeper/data"
	$(typeset -f createConfFile)
	createConfFile $numberOfServers $username ${nodeArray[*]};
	echo $i > "/local/$username/zookeeper/data/myid"
EOF
done

## Start the Zookeeper servers
for (( i=1; i <= $numberOfServers; i++))
do
	echo "Starting server in node ${nodeArray[$i-1]}"
ssh ${nodeArray[$i-1]} <<-EOF
	/local/$username/zookeeper/bin/zkServer.sh start
EOF

done

echo "Zookeeper cluster should be started"
