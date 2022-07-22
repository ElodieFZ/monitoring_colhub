#!/bin/bash -f
#$ -N dhus_logs
#$ -S /bin/bash
#$ -l h_rt=02:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/ppi
#$ -o /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/ppi

export PYTHONPATH=/home/nbs/colhub:/home/nbs/colhub/tools:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# Colhub monitoring: 
#   - check BEs logs for ingestion
#   - check FEs logs for downloads, number of users 
#
# Running as cron every night - DO NOT MODIFY -
#
# Outputs used in notebooks /home/nbs/notebooks/daily_monitoring/dhus_downloads.ipynb and dhus_logs.ipynb
#
# -------------------------------------------------------------

#set -x

d1=$(date -d '-1 day' +%Y%m%d)
d2=$(date -d '-1 day' +%Y%m%d)

logdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/colhub
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/monitoring/dhus_logs

# Check dhus instances logs

# -- FRONTENDS --
python /home/nbs/colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-global/logs/NBS -o ${monitdir} -sl ${logdir}
python /home/nbs/colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-global/logs/ESA -o ${monitdir} -sl ${logdir}
python /home/nbs/colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-frontend-AOI/logs/NBS -o ${monitdir} -sl ${logdir}

# -- BACKENDS --
area='global'
prods='S1 S2L1C S2L2A S2DEM S3 S5p'
for prod in $prods; do
    python /home/nbs/colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-backend-${area}/${prod}/logs -o ${monitdir} -sl ${logdir}
done

area='AOI'
prods='S1 S2L1C S2L2A S3 S5p'
for prod in $prods; do
    python /home/nbs/colhub/script/monitoring_logs_dhus -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/ESAcolhub/production-backend-${area}/${prod}/logs -o ${monitdir} -sl ${logdir}
done
