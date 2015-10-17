#!/bin/bash
#SBATCH --tasks-per-node=1
#SBATCH -N 2
#SBATCH --cpu_bind=verbose,cores
#SBATCH --exclusive
cd /data/scratch/sumanan
srun --exclusive  --nodes=2 ./bin/java/jdk1.7.0_40/bin/java -jar fmradio/StreamNode.jar 128.30.116.12
