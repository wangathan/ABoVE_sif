#!/bin/bash
#$ -pe omp 16
#$ -V

Rscript recombine.R
