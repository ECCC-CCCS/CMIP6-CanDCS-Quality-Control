#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  1 19:22:13 2021

@author: ***REMOVED***
"""

import xarray as xr
import glob, os
import numpy as np
import pandas as pd
import time
from datetime import timedelta
import dask
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

start_time = time.time()

ssp = ['ssp126','ssp245','ssp585'] # Name of the ssp

models = ['HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']          #  Input models
                
for mods in models:
    for s in ssp:
        Input = glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/'+mods+'_'+s+'_*')
        path = '/***REMOVED***/projects/CMIP6-BCCAQ-quality-control/***REMOVED***/Climdex_indices_figures/'+mods+'_'+s+'/'
        if not os.path.exists(path):
            os.makedirs(path)
        for folder in Input[:]:
            file_list=os.listdir(folder)
            file_list.sort()
            file_list=[folder+'/'+f for f in file_list]
            for fi in file_list:
                ds = xr.open_dataset(fi, decode_times=False)
                ds['time'] = xr.decode_cf(ds).time
                indice_list = list(ds.data_vars)
                indice = indice_list[0]
                new_data = ds[indice].sel(time=slice('1951-01-01','2101-01-01')).resample(time="AS").mean(dim="time")
                new_data["time"] = new_data.indexes["time"].to_datetimeindex()        #should be used for 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G'
                present_data = new_data.sel(time=slice('1981-01-01','2010-12-31')).mean(dim="time")
                plt.figure(figsize = (20, 10))
                plt.subplot(2,3,1)
                map1 = present_data.plot()
                plt.title('1981-2010 time-period')
                future_data = new_data.sel(time=slice('2071-01-01','2100-12-31')).mean(dim="time")
                plt.subplot(2,3,2)
                map2 = future_data.plot()
                plt.title('2071-2100 time-period')
                diff_result = future_data-present_data
                plt.subplot(2,3,3)
                map3 = diff_result.plot()
                plt.title('Difference between past & future')
                weights = np.cos(np.deg2rad(new_data.lat))
                data_weighted = new_data.weighted(weights)
                data_not_weighted = new_data
                weighted_mean = data_weighted.mean(dim=('lon', 'lat'))
                weighted_rolling_mean= weighted_mean.rolling(time=10, center=True).mean()
                plt.subplot(2,3,4)
                map5 = weighted_mean.plot()
                plt.title('Mean time-series weighted')
                plt.subplot(2,3,5)
                map6 = weighted_rolling_mean.plot()
                plt.title('Mean time-series weighted (10-year rolling)')
                plt.tight_layout()
                file_name = indice+'.png'
                plt.savefig(path+file_name)
                plt.clf()
                plt.close('all')

print("It took", str(timedelta(seconds=time.time() - start_time)), "to run")