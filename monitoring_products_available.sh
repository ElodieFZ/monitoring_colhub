#!/bin/bash -f
#$ -N colhub_logs
#$ -S /bin/bash
#$ -l h_rt=02:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /home/elodief/monitoring_colhub/logs
#$ -o /home/elodief/monitoring_colhub/logs

export PYTHONPATH=/home/elodief/monitoring_colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# Colhub monitoring: check BEs logs, request products on FEs
# Running as cron every night - DO NOT MODIFY -
#
# -------------------------------------------------------------

d1=$(date -d '-3 day' +%Y%m%d)
d2=$(date -d '-1 day' +%Y%m%d)

logdir=/home/elodief/monitoring_colhub/logs
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/monitoring/elodief/queries

# Request dhus frontends to check for number of products available 
# for one product and one sensing date

# For colhub global and scihub, request for two zones: global and AOI
hubs='colhub_global scihub'
areas='global colhub_aoi'
products='S1 S2L1C S2L2A S3 S5p'
for hub in $hubs; do
  for area in $areas; do
    for p in $products; do
      if [[ "$hub" == 'scihub' && "$p" == "S5p" ]]; then
        python /home/elodief/monitoring_colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_scihub.csv -dh s5phub -l ${logdir} -a ${area}
      else
        python /home/elodief/monitoring_colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_${hub}.csv -dh $hub -l ${logdir} -a ${area}
      fi
    done
  done
done

# For esa global and colhub AOI, request global data only
hubs='esahub_global colhub_AOI'
areas='global'
products='S1 S2L1C S2L2A S3 S5p'
for hub in $hubs; do
  for area in $areas; do
    for p in $products; do
      python /home/elodief/monitoring_colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_${hub}.csv -dh $hub -l ${logdir} -a ${area}
    done
  done
done

