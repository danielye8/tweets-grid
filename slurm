#!/bin/bash
#SBATCH --time=0:15:00
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1

module purge
module load python/3.9.0
module load foss/2019b
module load mpi4py/3.0.2-timed-pingpong

time srun python main.py

##DO NOT ADD/EDIT CODE BEYOND THIS LINE
##Job monitor command to list the resource usage
my-job-stats -a -n -s