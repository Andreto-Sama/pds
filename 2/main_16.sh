#!/bin/bash
#SBATCH --job-name=PDS_16
#SBATCH --partition=batch
#SBATCH --ntasks-per-node=16
#SBATCH --nodes=1
#SBATCH --time=00:10

module load gcc openmpi
mpicc -O3 mpi.c -o mpi.out

for i in {1..10}
do
srun ./mpi.out 32768 512 mnist.txt
done

echo "Done with 16 cores"
