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
	read -a rrr <<< "${dnsservers[28]}"
	echo ${rrr[0]}
	echo ${rrr[1]}
	nodeid=${rrr[1]:5}
	echo $nodeid
}

function verifyIP(){
	if [ $ip -eq 10 ]; then
		echo "Error in IP address finding"
		echo "Terminating the controller"
		scancel $jobid
		exit
	fi
}

out=$(sbatch controller.sh) #start the controller.
echo $out

jobid=$(jobId)
nodelistline=$(nodelistLine)
nodeid=0
nodeID

ip=$(($nodeid+10))	#In Lanka cluster, a node's ip address is 128.30.116.[Node number + 10]. 
			#Here $ip variable contains only the last byte of the ip address.
echo $ip
verifyIP
sbatch streamnode.sh $ip
