#!/bin/bash -l
#$ -V
#$ -l mem_total=30G
#$ -pe omp 16
#$ -l h_rt=20:00:00
#$ -j y

module purge
module load R_earth/3.1.0

Rscript readSIFnc_CO2.R
