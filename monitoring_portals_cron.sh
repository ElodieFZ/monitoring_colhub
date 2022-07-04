#!/bin/bash -f
#$ -N portals
#$ -S /bin/bash
#$ -l h_rt=02:30:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/ppi
#$ -o /lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/ppi

export PYTHONPATH=/home/nbs/colhub:$PYTHONPATH
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production-04-2021

# -------------------------------------------------------------
#
# Colhub monitoring: compare number products available on different portals
#
# Request several FEs (within and outside MET) to check how many products are available per sensing date and per product.
# Default is to check colhub FE, colhub ESA FE, scihub (used as monitoring reference), colhub AOI FE
#
# Running as cron every night - DO NOT MODIFY -
#
# -------------------------------------------------------------

##set -x

d1=$(date -d '-3 day' +%Y%m%d)
d2=$(date -d '-1 day' +%Y%m%d)

logdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/logs/colhub
monitdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/production/monitoring/dhus_queries

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
        python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_scihub.csv -dh s5phub -l ${logdir} -a ${area}
      else
        python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_${hub}.csv -dh $hub -l ${logdir} -a ${area}
      fi
    done
  done
done

# CODA - Query only S3 data over AOI
python /home/nbs/colhub/script/request_colhub -p S3 -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_coda.csv -dh coda -l ${logdir} -a colhub_aoi

# For esa global and colhub AOI, request global data only
hubs='esahub_global colhub_AOI'
areas='global'
products='S1 S2L1C S2L2A S3 S5p'
for hub in $hubs; do
  for area in $areas; do
    for p in $products; do
      python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_${hub}.csv -dh $hub -l ${logdir} -a ${area}
    done
  done
done

## 2022-04-07 - BE are now in more secure network so unavailable for direct queries
### For backends, request global data only
##products='S1 S2L1C S2L2A S3 S5p'
##areas='global AOI'
##for area in $areas; do
##  for p in $products; do
##    python /home/nbs/colhub/script/request_colhub -p ${p} -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_BE_${p}_${area}.csv -dh BE_${p}_${area} -l ${logdir} -a global
##  done
##done
##python /home/nbs/colhub/script/request_colhub -p S2L1C -d1 $d1 -d2 $d2 -wn True -of ${monitdir}/products_in_BE_S2DEM_global.csv -dh BE_S2DEM_global -l ${logdir} -a global

