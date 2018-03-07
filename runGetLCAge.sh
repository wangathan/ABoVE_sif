#!/bin/bash -l
#$ -V
#$ -l mem_total=30G
#$ -l h_rt=60:00:00
#$ -pe omp 16

module purge
module load R_earth/3.1.0

Rscript getLC_age.R $1
