#!/bin/bash -f
#$ -N q_s2_2015
#$ -S /bin/bash
#$ -l h_rt=05:00:00
#$ -l h_vmem=2G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y
#$ -wd /home/nbs/file_conversion_elf/nbs_tools
#$ -o /home/nbs/file_conversion_elf/logs

export PYTHONPATH=/home/nbs/file_conversion_elf/nbs_tools/tools:$PYTHONPATH
module add conda/production
source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate production

cfg='s2'

year='2015'
start='2015-01-01'
end='2016-01-01'

while ! [[ $start > $end ]]; do 

    d1=$(date -d "$start" +%Y%m)
    d2=$(date -d "$start + 1 month" +%Y%m)

    echo "Processing $d1 $d2"

    python /home/nbs/file_conversion_elf/nbs_tools/script/request_colhub -p $cfg -s ${d1}01-${d2}01 -o /home/nbs/file_conversion_elf/monitoring/list_${cfg}_${year}_${d1}_${d2}.txt -l /home/nbs/file_conversion_elf/logs
    
    start=$(date -d "$start + 1 month" +%F)

done
