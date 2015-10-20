#!/bin/bash
#Author - Sumanan
#Feb 9, 2015
#Setup directories and scripts to run a distributed StreamJit app.
#Specifically, creates appDir, creates scripts(controller.sh and streamnode.sh)
#and finally, calls run.sh.
function createCTRLRSh(){
	runfile="controller.sh"
	res=$(get_prop "./options.properties" "tune")
	echo "#!/bin/bash" > $runfile
	echo "#SBATCH --tasks-per-node=1" >> $runfile
	echo "#SBATCH -N 1"  >> $runfile
	echo "#SBATCH --cpu_bind=verbose,cores" >> $runfile
	echo "#SBATCH --exclusive" >> $runfile
	echo "cd /data/scratch/sumanan/"$1 >> $runfile
	if [ "$res" -eq "1" ];then
		echo "mkdir -p $2" >> $runfile
		echo "cd $2" >> $runfile
		echo "srun python ../lib/opentuner/streamjit/streamjit2.py 12563 &" >> $runfile
		echo "cd .." >> $runfile
	fi
	echo "srun -l ../bin/java/jdk1.8.0_31/bin/java -Xmx120G -XX:InitialCodeCacheSize=1G -XX:ReservedCodeCacheSize=2G -jar $1.jar $3" >> $runfile
}

function createSNSh(){
	runfile="streamnode.sh"
	echo "#!/bin/bash" > $runfile
	echo "#SBATCH --tasks-per-node=1" >> $runfile
	echo "#SBATCH -N $2"  >> $runfile
	echo "#SBATCH --cpu_bind=verbose,cores" >> $runfile
	echo "#SBATCH --exclusive" >> $runfile
	echo 'args=("$@")' >> $runfile
	echo 'ip=${args[0]}' >> $runfile
	echo "cd /data/scratch/sumanan/"$1 >> $runfile
	echo "srun --exclusive  --nodes=$2 ../bin/java/jdk1.8.0_31/bin/java -Xmx120G -jar -XX:InitialCodeCacheSize=1G -XX:ReservedCodeCacheSize=2G StreamNode.jar 128.30.116."'$ip' >> $runfile
}

function creatdirs(){
	mkdir -p $1
	ln -s /data/scratch/sumanan/data $1/data
	ln -s /data/scratch/sumanan/lib $1/lib
	cd $1
}

get_prop(){
	grep  "^${2}=" ${1}| sed "s%${2}=\(.*\)%\1%"
}

if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters"
	echo "3 arguments must be passed"
	echo "setup.sh <app> <mainClass> <noOfnodes>"
	exit
fi

args=("$@")
app=${args[0]}
mainClass=${args[1]}
nodes=${args[2]}
totalNodes=$((nodes + 1))
cd /data/scratch/sumanan
creatdirs $app			# Changes the current working directory(CWD) as well.
mv "optionsLanka.properties" "options.properties"
createCTRLRSh $app $mainClass $totalNodes
createSNSh $app $nodes
cp ../run.sh run.sh
./run.sh
