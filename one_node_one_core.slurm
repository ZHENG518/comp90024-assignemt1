#!/bin/bash

#SBATCH --job-name=one_node_one_core
#SBATCH -p snowy
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
module add foss/2019b
module add python/3.7.4

srun -n 1 python3 main.py