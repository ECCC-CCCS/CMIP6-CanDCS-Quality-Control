#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 19:42:45 2021

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
import xclim as xc
from xclim import ensembles
from pathlib import Path
import seaborn as sns

start_time = time.time()

ssp = ['ssp126','ssp245','ssp585'] # All emission scenarios


models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']


indices = ['csdiETCCDI','fdETCCDI','gslETCCDI','idETCCDI','prcptotETCCDI',
           'r10mmETCCDI','r1mmETCCDI','r20mmETCCDI','r95daysETCCDI','r95pETCCDI','r99daysETCCDI',
           'r99pETCCDI','sdiiETCCDI','su30ETCCDI','suETCCDI','wsdiETCCDI'] #'trETCCDI_' missing  (The script needs to be slightly changed as input will include both trETCCDI and dtrETCCDI indices)

# indices = ['cddETCCDI','cwdETCCDI','rx1dayETCCDI','rx2dayETCCDI','rx5dayETCCDI','txnETCCDI','txxETCCDI'] # Monthly MAXIMUM instead of mean of variables
# indices = ['tnnETCCDI','tnxETCCDI'] # Monthly MINIMUM instead of mean of variables
# indices = ['dtrETCCDI','tn10pETCCDI','tn90pETCCDI','tx10pETCCDI','tx90pETCCDI'] # Monthly MEAN

thedic = {'cddETCCDI':'maximum number of consecutive days with PRCP < 1mm','csdiETCCDI':'Cold spell duration index',
          'cwdETCCDI':'maximum number of consecutive days with PRCP ≥ 1mm','fdETCCDI':'Number of frost days',
          'gslETCCDI':'Growing season length','idETCCDI':'Number of icing days','prcptotETCCDI':'Annual total precipitation in wet days',
          'r10mmETCCDI':'Annual count of days when PRCP ≥ 10mm','r1mmETCCDI':'Annual count of days when daily precipitation amount (PRCP) ≥ 1mm',
          'r20mmETCCDI':'Annual count of days when PRCP ≥ 20mm','r95daysETCCDI':'Number of days where daily precipitation exceeds the 95th percentile',
          'r95pETCCDI':'Annual total accumulation of precipitation on wet days (PRCP > 95th percentile)','r99daysETCCDI':'Number of days where daily precipitation exceeds the 99th percentile',
          'r99pETCCDI':'Annual total accumulation of precipitation on very wet days (PRCP > 99th percentile)','sdiiETCCDI':'Simple precipitation intensity index',
          'su30ETCCDI':'Number of summer days (> 30°C)','suETCCDI':'Number of summer days (> 25°C)','wsdiETCCDI':'Warm spell duration index',
          'trETCCDI':'Number of tropical nights','dtrETCCDI':'Daily temperature range','tn10pETCCDI':'Percentage of days when TN < 10th percentile',
          'tn90pETCCDI':'Percentage of days when TN > 90th percentile','tx10pETCCDI':'Percentage of days when TX < 10th percentile',
          'tx90pETCCDI':'Percentage of days when TX > 90th percentile','tnnETCCDI':'Monthly minimum value of TN','tnxETCCDI':'Monthly maximum value of daily minimum temperature (TN)',
          'rx1dayETCCDI':'Monthly maximum 1-day precipitation','rx2dayETCCDI':'Monthly maximum consecutive 2-day precipitation','rx5dayETCCDI':'Monthly maximum consecutive 5-day precipitation',
          'txnETCCDI':'Monthly minimum value of TX','txxETCCDI':'Monthly maximum value of daily maximum temperature (TX)'}

thedic1 = {'cddETCCDI':'Days','csdiETCCDI':'Days','cwdETCCDI':'Days','fdETCCDI':'Days','gslETCCDI':'Days','idETCCDI':'Days','prcptotETCCDI':'Millimeter',
          'r10mmETCCDI':'Days','r1mmETCCDI':'Days','r20mmETCCDI':'Days','r95daysETCCDI':'Days','r95pETCCDI':'Millimeter','r99daysETCCDI':'Days',
          'r99pETCCDI':'Millimeter','sdiiETCCDI':'Millimeter','su30ETCCDI':'Days','suETCCDI':'Days','wsdiETCCDI':'Days','trETCCDI_':'Days','dtrETCCDI':'°C','tn10pETCCDI':'%',
          'tn90pETCCDI':'%','tx10pETCCDI':'%','tx90pETCCDI':'%','tnnETCCDI':'°C','tnxETCCDI':'°C','rx1dayETCCDI':'Millimeter','rx2dayETCCDI':'Millimeter','rx5dayETCCDI':'Millimeter',
          'txnETCCDI':'°C','txxETCCDI':'°C'}


path = '/***REMOVED***/projects/CMIP6-BCCAQ-quality-control/***REMOVED***/ensemble_percentile_new/'    # Change the path for saving the out file

for i in indices:
    for s in ssp:
        ncfiles=[]
        for mods in models:
            if mods == 'CanESM5':
                Input = sorted(glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/'+mods+'_'+s+'_r10i1p2f1/' ))
            else:
                Input = sorted(glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/'+mods+'_'+s+'_*/'))
            for folder in Input:
                file_list = os.listdir(folder)
                file_list.sort()
                matching = [f for f in file_list if i in f]
                for m in matching:
                    all_files = folder+m
                    ds = xr.open_dataset(all_files, decode_times=False)
                    ds['time'] = xr.decode_cf(ds).time
                    indice_list = list(ds.data_vars)
                    indice = indice_list[0]
                    weights = np.cos(np.deg2rad(ds.lat))
                    new_data = ds[indice].resample(time="AS").mean(dim="time")  # change for MAX and MIN
                    data_weighted = new_data.weighted(weights)
                    weighted_mean = data_weighted.mean(dim=('lon', 'lat'))
                    mean_file = weighted_mean.to_dataset() 
                    ncfiles.append(mean_file)                
        xr.set_options(display_style="html", display_width=50)
        ens = ensembles.create_ensemble(ncfiles)
        if s == 'ssp126':
            ens_perc_ssp126 = ensembles.ensemble_percentiles(ens, values=[10, 50, 90], split=False)
        elif s == 'ssp245':
            ens_perc_ssp245 = ensembles.ensemble_percentiles(ens, values=[10, 50, 90], split=False)
        else:
            ens_perc_ssp585 = ensembles.ensemble_percentiles(ens, values=[10, 50, 90], split=False)
    plt.style.use("seaborn-dark")
    plt.rcParams["figure.figsize"] = (15, 7)
    fig, ax = plt.subplots(1)
    ax.fill_between(ens_perc_ssp126.time.values, ens_perc_ssp126[i].sel(percentiles=10).values, 
                    ens_perc_ssp126[i].sel(percentiles=90).values, facecolor='#377eb8', alpha=0.4, label="ssp126_10th to 90th percentile range")
    ax._get_lines.get_next_color()
    ax._get_lines.get_next_color()
    ax.fill_between(ens_perc_ssp245.time.values, ens_perc_ssp245[i].sel(percentiles=10).values, 
                    ens_perc_ssp245[i].sel(percentiles=90).values, facecolor='#dede00', alpha=0.4, label="ssp245_10th to 90th percentile range")
    ax._get_lines.get_next_color()
    ax._get_lines.get_next_color()
    ax.fill_between(ens_perc_ssp585.time.values, ens_perc_ssp585[i].sel(percentiles=10).values, 
                    ens_perc_ssp585[i].sel(percentiles=90).values, facecolor='#a65628', alpha=0.4, label="ssp585_10th to 90th percentile range")
    ax._get_lines.get_next_color()
    ax._get_lines.get_next_color()
    ax.plot(ens_perc_ssp126.time.values, ens_perc_ssp126[i].sel(percentiles=50).values, linewidth=3, color='#377eb8', label="ssp126_Median")
    ax.plot(ens_perc_ssp245.time.values, ens_perc_ssp245[i].sel(percentiles=50).values, linewidth=3, color='#dede00', label="ssp245_Median")   
    ax.plot(ens_perc_ssp585.time.values, ens_perc_ssp585[i].sel(percentiles=50).values, linewidth=3, color='#a65628', label="ssp585_Median")
    ax.legend(loc='upper left')
    title = i+'_'+thedic[i]
    y_axis = thedic1[i]
    fig.suptitle(title, fontsize=15)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel(y_axis, fontsize=12)
    file_name = indice+'_Annual Mean.png'
    fig1 = plt.gcf()
    plt.show()
    fig1.savefig(path+file_name)
    plt.clf()
    plt.close('all')
    
print("It took", str(timedelta(seconds=time.time() - start_time)), "to run")