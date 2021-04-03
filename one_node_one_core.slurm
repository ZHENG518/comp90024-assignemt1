#!/bin/bash

#SBATCH --job-name=one_node_one_core
#SBATCH -p physical
#SBATCH -N 1
#SBATCH --ntasks-per-node=8
module add foss/2019b
module add python/3.7.4

srun -n 8 python3 main.py