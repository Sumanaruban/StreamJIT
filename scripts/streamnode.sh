#!/bin/bash
#SBATCH --tasks-per-node=1
#SBATCH -N 2
#SBATCH --cpu_bind=verbose,cores
#SBATCH --exclusive
cd /data/scratch/sumanan/FMRadio
srun --exclusive  --nodes=2 ../bin/java/jdk1.8.0_25/bin/java -Xmx2048m -jar StreamNode.jar 128.30.116.17
