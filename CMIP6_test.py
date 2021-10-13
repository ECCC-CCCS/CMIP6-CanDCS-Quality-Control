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
import dask

# Name of the variable, the ssp and model 
ssp = ['ssp126','ssp245','ssp585']

variables = ['tasmax', 'pr', 'tasmin']
"""
models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'FGOALS-g3', 'GFDL-ESM4', 'HadGEM3-GC31-LL',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KACE-1-0-G', 'KIOST-ESM',
           'MIROC-ES2L', 'MIROC6']

"""
#models = ['ACCESS-CM2', 'ACCESS-ESM1-5']

models = ['ACCESS-ESM1-5']

list_netcdf=[]

for mods in models:
    for var in variables:
            for s in ssp:
                Input = glob.glob('/***REMOVED***/ccdp/scrd901/CMIP6_bccaqv2datasets/'+mods+'_'+var+'*_'+s+'_5_year_files')
                #print(Input)
                for folder in Input[:]:
                    file_list=os.listdir(folder)
                    print('working on folder:' + folder)
                    file_list=[folder+'/'+f for f in file_list if '.nc' in f and not 'foQXts' in f]
                    #print(file_list)
                    ds=xr.open_mfdataset(file_list)
                    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
                        Annual_mean = ds.groupby('time.year').mean(dim=('lon','lat','time'))
                        print(Annual_mean) #printing
                            
