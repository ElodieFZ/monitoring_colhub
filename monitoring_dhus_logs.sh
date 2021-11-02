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

#set -x

#d1=$(date -d '-3 day' +%Y%m%d)
d1=$(date -d '-1 day' +%Y%m%d)
d2=$(date -d '-1 day' +%Y%m%d)

logdir=/home/elodief/monitoring_colhub/logs
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/monitoring/elodief

# Check dhus instances logs

# -- FRONTENDS --
python /home/elodief/monitoring_colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-global/logs/NBS -o ${monitdir} -sl ${logdir}
python /home/elodief/monitoring_colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-global/logs/ESA -o ${monitdir} -sl ${logdir}
python /home/elodief/monitoring_colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-AOI/logs/NBS -o ${monitdir} -sl ${logdir}

# -- BACKENDS --
area='global'
prods='S1 S2L1C S2L2A S2DEM S3 S5p'
for prod in $prods; do
    python /home/elodief/monitoring_colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-backend-${area}/${prod}/logs -o ${monitdir} -sl ${logdir}
done

area='AOI'
prods='S1 S2L1C S2L2A S3 S5p'
for prod in $prods; do
    python /home/elodief/monitoring_colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-backend-${area}/${prod}/logs -o ${monitdir} -sl ${logdir}
done
