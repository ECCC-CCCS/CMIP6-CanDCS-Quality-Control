import xarray as xr
import glob, os
import numpy as np
import pandas as pd
import time
from datetime import timedelta
import dask
import xclim as xc
from xclim import ensembles
from pathlib import Path



ssp = ['ssp126','ssp245','ssp585'] # All 3 ssps, 'ssp126','ssp245','ssp585'


models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'HadGEM3-GC31-LL', 'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR','KACE-1-0-G', 'KIOST-ESM',
           'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
           'NorESM2-MM', 'TaiESM1', 'UKESM1-0-LL' ]         
           
indices = ['csdiETCCDI','cwdETCCDI','dtrETCCDI','fdETCCDI','gslETCCDI','idETCCDI','prcptotETCCDI',
          'r10mmETCCDI','r1mmETCCDI','r20mmETCCDI','r95daysETCCDI','r95pETCCDI','r99daysETCCDI','r99pETCCDI',
          'rx1dayETCCDI','rx2dayETCCDI','rx5dayETCCDI','sdiiETCCDI','su30ETCCDI','suETCCDI','tn10pETCCDI',
          'tn90pETCCDI','tnnETCCDI','tnxETCCDI','trETCCDI','tx10pETCCDI','tx90pETCCDI','txnETCCDI','txxETCCDI',
          'wsdiETCCDI'] #There are 31 r1i1p2f1

outputpath = 'path to output folder"
inputpath = 'path to input folder"

for i in indices:
    print(i)
    output = outputpath+i+'/'
    if not os.path.exists(output):
        os.makedirs(output) 
    else:
        pass
    for s in ssp:
        ncfiles=[]
        for mods in models:
            if mods == 'CanESM5':
                Input = sorted(glob.glob(inputpath+mods+'_'+s+'_r10i1p2f1/' ))
            else:
                Input = sorted(glob.glob(inputpath+mods+'_'+s+'_*/'))
            for folder in Input:
                file_list = os.listdir(folder)
                file_list.sort()
                matching = [f for f in file_list if i in f]
                for m in matching:
                    all_files = folder+m
                    ncfiles.append(all_files)
        output = outputpath+i+'/'
        ens = ensembles.create_ensemble(ncfiles)
        ens_percs = ensembles.ensemble_percentiles(ens, values=(10, 50, 90))
        ens_percs.to_zarr(output+i+'_'+s+'.zarr', mode ='w')
        