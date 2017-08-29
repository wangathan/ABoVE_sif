#!/bin/bash -l
#$ -V
#$ -l mem_total=20G
#$ -pe omp 16
#$ -l h_rt=20:00:00

module purge
module load R_earth/3.1.0

Rscript readSIFnc.R
