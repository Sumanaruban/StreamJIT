#!/bin/bash
#SBATCH --tasks-per-node=1
#SBATCH -N 1 
#SBATCH --cpu_bind=verbose,cores
#SBATCH --exclusive
cd /data/scratch/sumanan/FMRadio
mkdir -p FMRadioCore
cd FMRadioCore
srun python ../autotuner/streamjit2.py 12563 &
cd ..
srun -l ../bin/java/jdk1.8.0_31/bin/java -Xmx120G -XX:InitialCodeCacheSize=1G -XX:ReservedCodeCacheSize=2G -jar FMRadio.jar 3
