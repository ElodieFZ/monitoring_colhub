#!/bin/bash -f
#$ -N colhub_check
#$ -S /bin/bash
#$ -l h_rt=00:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/ppi
#$ -o /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/ppi

export PYTHONPATH=/home/nbs/colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------
#
# Monitor colhub: 
#     check for one date (sensing date) the nb of products available on different hubs
#
# -------------------------------------

logdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/colhub
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/monitoring/tmp

day=20211013 # YYYYMMDD
hubs='colhub_nbs scihub_elodief colhub_new_elodief'
hubs='colhub_BE_S1'

# Which products/area to check
# (all_global / all_AOI / other cfg file in /home/nbs/colhub/cfg/products)
product='S1_global'

for hub in $hubs; do
    python /home/nbs/colhub/script/request_colhub -p $product -sd1 $day -wn True -on ${monitdir}/check_${day}_${hub}.csv -dh $hub -l ${logdir}
done

