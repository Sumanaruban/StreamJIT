#!/bin/bash
#Author - Sumanan
#Oct 19, 2015
#Starts a controller node by calling "sbatch controller.sh", from the sbatch's
#output string finds the jobId of the controller process, from the jobId finds
#the ip address of the node where the controller is running and finally, starts
#streamnodes with the ip address of the controller node by calling
#"sbatch streamnode.sh $ip".

# Extracts jobID from slurm.sbatch's return string.
function jobId(){
	IFS=' '
	read -a outsplit <<< "${out}"
	jobid=${outsplit[3]}
	echo $jobid
}

# Returns the line that contains nodelist info in "scontrol show job $jobid"'s output.
function nodelistLine(){
	ddd=$(scontrol show job $jobid)
	eee=$(echo $ddd | tr '\n' ' ') #ddd contains newlines. Lets remove all newlines.
	IFS=' '
	read -a lines <<< "${eee}"
	echo ${lines[28]}
}

function nodeID(){
	IFS='='
	read -a rrr <<< "${nodelistline}"
	id=${rrr[1]:5}
	echo $id
}

function verifyIP(){
	if [ $ip -eq 10 ]; then
		echo "Error in IP address finding"
		echo "Terminating the controller"
		scancel $jobid
		exit
	fi
}

args=("$@")
snInstances=${args[0]}
if ["$snInstances" = ""]; then
	snInstances=1
fi

out=$(sbatch controller.sh) #start the controller.
echo $out

jobid=$(jobId)
nodelistline=$(nodelistLine)
nodeid=$(nodeID)

ip=$(($nodeid+10))	#In Lanka cluster, a node's ip address is 128.30.116.[Node number + 10]. 
			#Here $ip variable contains only the last byte of the ip address.
echo $ip
verifyIP

for (( c=0; c<$snInstances; c++ ))
do
   sbatch streamnode.sh $ip
done
