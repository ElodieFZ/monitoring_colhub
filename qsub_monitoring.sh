#!/bin/bash -f
#$ -N colhub_monitor
#$ -S /bin/bash
#$ -l h_rt=01:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/ppi
#$ -o /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/ppi

export PYTHONPATH=/home/nbs/colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# Colhub monitoring: check BEs logs, request products on FEs
# Running as cron every night - DO NOT MODIFY -
#
# -------------------------------------------------------------

yesterday=$(date -d '-1 day' +%Y%m%d)
threedaysago=$(date -d '-3 day' +%Y%m%d)

#threedaysago=20211001

logdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/logs/colhub
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/prod_tmp/monitoring

# Check BEs logs
python /home/nbs/colhub/script/monitoring_logs_BE -p all -d1 $yesterday -o ${monitdir}/colhub_logs_global_BE.csv -sl ${logdir}
python /home/nbs/colhub/script/monitoring_logs_BE -p all -d1 $yesterday -cl /lustre/storeB/project/ESAcolhub/production-backend-AOI -o ${monitdir}/colhub_logs_AOI_BE.csv -sl ${logdir}

# Query BACKENDS for data with sensing date = yesterday and 3 days ago
areas='global AOI'
prods='S1 S2L1C S2L2A S3 S5p'
for area in $areas; do
    for prod in $prods; do
        # ---- BE ----
        # search products in all area instead of AOI for BE -> this way we can check that we indeed only have AOI data in AOI backend
        python /home/nbs/colhub/script/request_colhub -p ${prod}_global -sd1 $threedaysago -sd2 $yesterday -wn True -on ${monitdir}/colhub_query_${area}_BE_${prod}.csv -dh colhub_BE_${prod}_${area} -l ${logdir}
        # ---- FE colhub old ----
        python /home/nbs/colhub/script/request_colhub -p ${prod}_${area} -sd1 $threedaysago -sd2 $yesterday -wn True -on ${monitdir}/colhub_query_${area}_FE_colhub_old_${prod}.csv -dh colhub_nbs -l ${logdir}
        # ---- FE colhub new ----
        python /home/nbs/colhub/script/request_colhub -p ${prod}_${area} -sd1 $threedaysago -sd2 $yesterday -wn True -on ${monitdir}/colhub_query_${area}_FE_colhub_${prod}.csv -dh colhub_new_elodief -l ${logdir}
        # ---- FE scihub ----
        if [[ "$prod" == "S5p" ]]; then
            python /home/nbs/colhub/script/request_colhub -p ${prod}_${area} -sd1 $threedaysago -sd2 $yesterday -wn True -on ${monitdir}/colhub_query_${area}_FE_scihub_${prod}.csv -dh s5phub -l ${logdir}
        else
            python /home/nbs/colhub/script/request_colhub -p ${prod}_${area} -sd1 $threedaysago -sd2 $yesterday -wn True -on ${monitdir}/colhub_query_${area}_FE_scihub_${prod}.csv -dh scihub_elodief -l ${logdir}
        fi
    done
done
