#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 12 19:58:57 2021

@author: ***REMOVED***
"""

import xarray as xr
import glob, os
import numpy as np
import pandas as pd
import time
from datetime import timedelta
import dask

start_time = time.time()

# Name of the variable, the ssp and model 
ssp = ['ssp126','ssp245','ssp585']

variables = ['tasmax', 'tasmin', 'pr']


models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4', 'HadGEM3-GC31-LL',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KACE-1-0-G', 'KIOST-ESM',
           'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
           'NorESM2-MM', 'TaiESM1', 'UKESM1-0-LL']          # total of 26 models

for mods in models:
    list_netcdf=[]
    for s in ssp:
        for var in variables:
            Input = glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/'+mods+'_'+var+'*_'+s+'_5_year_files')
            for folder in Input[:]:
                file_list=os.listdir(folder)
                file_list.sort()
                print('working on folder:' + folder)
                file_list=[folder+'/'+f for f in file_list if '.html' not in f and not '.nc.' in f]
                print(file_list)
                ds=xr.open_mfdataset(file_list, parallel=True)
                rename= ds.rename({var:var+'_'+s})
                list_netcdf.append(rename)
                new_dataset = xr.combine_by_coords(list_netcdf, combine_attrs="drop")
                final_dataset = new_dataset.groupby('time.year').mean()
                final_dataset.to_netcdf('/***REMOVED***/***REMOVED***/CMIP6_Test/ssp126/'+mods+'_150_year_mean.nc')

print("It took", str(timedelta(seconds=time.time() - start_time)), "to run")
                            
