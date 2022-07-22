#!/bin/bash -f
#$ -N compare_hubs
#$ -S /bin/bash
#$ -l h_rt=20:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /home/nbs/colhub
#$ -o /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/data_gaps/data_gaps_20220609/logs

export PYTHONPATH=/home/nbs/colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# List data available on datahubs for a product, a time period and an area
# Output = text file for each hub / product
#
# -------------------------------------------------------------

set -x

year=2020

d1=${year}0101
d2=${year}1231

#d1=20180101
#d2=20220615

#monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/monitoring/data_gaps_barkebille/updated
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/data_gaps/data_gaps_20220609
logdir=$monitdir/logs

hubs='scihub colhub_AOI'
products='S2L1C S1 S2L2A S3'
area='colhub_aoi'
for hub in $hubs; do
  for p in $products; do
    mkdir $monitdir/$year/$p
    if [[ "$hub" == "scihub" && "$p" == "S5p" ]]; then
      python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wl True -ld ${monitdir}/$year/$p -dh s5phub -l ${logdir} -a ${area} -wn False
    else
      python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wl True -ld ${monitdir}/$year/$p -dh $hub -l ${logdir} -a ${area} -wn False
    fi
  done
done

