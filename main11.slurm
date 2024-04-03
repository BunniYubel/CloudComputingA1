#!/bin/bash
#SBATCH --partition=sapphire
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=01:00:00

module purge
module load foss/2022a
module load Python/3.10.4
module load mpi4py/3.1.4
source ~/venv/bin/activate
pip install ijson

srun python3 main.py

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s

