#! /bin/bash

set -eax
# 



list_ssp='ssp585'
memb=r1i1p1f1

list_model='ACCESS-CM2 BCC-CSM2-MR CMCC-ESN2 FGOALS-g3 INM-CM5-0 MIROC6 NorESM2-MM'
list_var='pr'

output="output path"

if [ ! -d  ${output} ]
then
    echo 'making directory'
    mkdir ${output}
fi

# start the work by looing over models and variables
for ssp in $list_ssp; do
echo ssp
for model in $list_model ; do
for var in $list_var ; do
    
inputpath =  "input path"   
input=${inputpath}/${model}_${var}_${memb}_${ssp}_5_year_files
cd ${input}


for fld in ${var}_day_BCCAQv2+ANUSPLIN300_${model}_historical+${ssp}_${memb}_*
do

cdo -b 64 yearsum ${fld} ${output}/annual_${fld}

done

ncrcat ${output}/annual_* ${output}/${var}_ysum_${model}_${ssp}_1950-2100.nc
rm ${output}/annual_*

ncatted -h -a history,global,d,, ${output}/${var}_ysum_${model}_${ssp}_1950-2100.nc

done
done
done
#############################################


echo  ' *******END*******'
