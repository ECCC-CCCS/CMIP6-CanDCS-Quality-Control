#! /bin/bash

set -eax
# 

ssp=ssp245
memb=r1i1p1f1

list_model='BCC-CSM2-MR CanESM5'
list_var='tasmin tasmax'

output= "output path" 

if [ ! -d  ${output} ]
then
    mkdir ${output}
fi

# start the work by looing over models and variables
for model in $list_model ; do
for var in $list_var ; do

inputpath =  "input path"   
input=${inputpath}/${model}_${var}_${memb}_${ssp}_5_year_files
cd ${input}


for fld in ${var}_day_BCCAQv2+ANUSPLIN300_${model}_historical+${ssp}_${memb}_*
do

cdo yearmean ${fld} ${output}/annual_${fld}

done

ncrcat ${output}/annual_* ${output}/${var}_ymean_${model}_${ssp}_1950-2100.nc
rm ${output}/annual_*

ncatted -h -a history,global,d,, ${output}/${var}_ymean_${model}_${ssp}_1950-2100.nc



done
#############################################


echo  ' *******END*******'







