#!/bin/bash -f
#$ -N colhub_logs
#$ -S /bin/bash
#$ -l h_rt=02:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /home/elodief/colhub/logs
#$ -o /home/elodief/colhub/logs

export PYTHONPATH=/home/elodief/colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# Colhub monitoring: check BEs logs, request products on FEs
# Running as cron every night - DO NOT MODIFY -
#
# -------------------------------------------------------------

#set -x

d1=20210930
d2=20211028

logdir=/home/elodief/colhub/logs
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/monitoring/elodief/old_dhus/new

# Check dhus instances logs

# -- FRONTENDS --
python /home/elodief/colhub/script/monitoring_logs_dhus_old_instance -d1 $d1 -d2 $d2 -dl /lustre/storeB/project/NBS/sentinel/production/logs/ -o ${monitdir} -sl ${logdir}
