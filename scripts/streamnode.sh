#!/bin/bash
#SBATCH --tasks-per-node=1
#SBATCH -N 2
#SBATCH --cpu_bind=verbose,cores
#SBATCH --exclusive
args=("$@")
ip=${args[0]}
cd /data/scratch/sumanan/FMRadio
srun --exclusive  --nodes=2 ../bin/java/jdk1.8.0_31/bin/java -Xmx120G -jar -XX:InitialCodeCacheSize=1G -XX:ReservedCodeCacheSize=2G StreamNode.jar 128.30.116.$ip
