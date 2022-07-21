#!/bin/bash -f
#$ -N colhub_download
#$ -S /bin/bash
#$ -l h_rt=3:00:00
#$ -l h_vmem=5G
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
#  Download products from scihub
#
# -------------------------------------------------------------

##set -x

sat=S1

listdir=/lustre/storeB/project/NBS2/sentinel/production/NorwAREA/netCDFNBS_work/data_gaps/data_gaps_20220609/2021/$sat
logdir=$listdir/logs
lockfile=/home/nbs/colhub/downloading_${sat}.now

mkdir -p $logdir
if [ -f $lockfile ]; then
    echo 'Script alread running - so exiting'
    exit 0
else
    touch $lockfile
    python /home/nbs/colhub/script/download_scihub -dh scihub_sentinelAPI -p $listdir/in_progress.csv -p1 $listdir/products_list_in_scihub.csv -p2 $listdir/products_list_in_colhub_AOI.csv -o $listdir/data -l $logdir -u elodief
    echo 'Script exiting OK'
    rm $lockfile
    echo "$lockfile has been removed"
fi
