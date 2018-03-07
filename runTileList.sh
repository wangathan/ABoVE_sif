#!/bin/bash -l
#$ -V
#$ -l mem_total=40G
#$ -l h_rt=10:00:00
#$ -pe omp 28

module purge
module load R_earth/3.1.0

Rscript getTileList.R $1
