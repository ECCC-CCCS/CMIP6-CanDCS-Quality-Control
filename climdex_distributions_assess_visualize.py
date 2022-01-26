# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 16:00:34 2022

@author: VanVlietL
"""
import xarray as xr
import glob
import gc
from scipy import stats
from tqdm import tqdm
import pandas as pd
import numpy as np 

scenarios = ['ssp126','ssp245','ssp585'] # All emission scenarios

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']

CMIP6_dic = {'cddETCCDI':'maximum number of consecutive days with PRCP < 1mm',
             'csdiETCCDI':'Cold spell duration index',
             'cwdETCCDI':'maximum number of consecutive days with PRCP ≥ 1mm',
             'fdETCCDI':'Number of frost days',
             'gslETCCDI':'Growing season length','idETCCDI':'Number of icing days',
             'prcptotETCCDI':'Annual total precipitation in wet days',
             'r10mmETCCDI':'Annual count of days when PRCP ≥ 10mm',
             'r1mmETCCDI':'Annual count of days when daily precipitation amount (PRCP) ≥ 1mm',
             'r20mmETCCDI':'Annual count of days when PRCP ≥ 20mm',
             'r95daysETCCDI':'Number of days where daily precipitation exceeds the 95th percentile',
             'r95pETCCDI':'Annual total accumulation of precipitation on wet days (PRCP > 95th percentile)','r99daysETCCDI':'Number of days where daily precipitation exceeds the 99th percentile',
             'r99pETCCDI':'Annual total accumulation of precipitation on very wet days (PRCP > 99th percentile)','sdiiETCCDI':'Simple precipitation intensity index',
             'su30ETCCDI':'Number of summer days (> 30°C)','suETCCDI':'Number of summer days (> 25°C)','wsdiETCCDI':'Warm spell duration index',
             'trETCCDI':'Number of tropical nights',
             'dtrETCCDI':'Daily temperature range','tn10pETCCDI':'Percentage of days when TN < 10th percentile',
             'tn90pETCCDI':'Percentage of days when TN > 90th percentile',
             'tx10pETCCDI':'Percentage of days when TX < 10th percentile',
             'tx90pETCCDI':'Percentage of days when TX > 90th percentile',
             'tnnETCCDI':'Monthly minimum value of TN',
             'tnxETCCDI':'Monthly maximum value of daily minimum temperature (TN)',
             'rx1dayETCCDI':'Monthly maximum 1-day precipitation',
             'rx2dayETCCDI':'Monthly maximum consecutive 2-day precipitation',
             'rx5dayETCCDI':'Monthly maximum consecutive 5-day precipitation',
             'txnETCCDI':'Monthly minimum value of TX',
             'txxETCCDI':'Monthly maximum value of daily maximum temperature (TX)'}

CMIP6_dic1 = {'cddETCCDI':'Days','csdiETCCDI':'Days','cwdETCCDI':'Days','fdETCCDI':'Days','gslETCCDI':'Days','idETCCDI':'Days','prcptotETCCDI':'Millimeter',
              'r10mmETCCDI':'Days','r1mmETCCDI':'Days','r20mmETCCDI':'Days','r95daysETCCDI':'Days','r95pETCCDI':'Millimeter','r99daysETCCDI':'Days',
              'r99pETCCDI':'Millimeter','sdiiETCCDI':'Millimeter','su30ETCCDI':'Days','suETCCDI':'Days','wsdiETCCDI':'Days','trETCCDI_':'Days','dtrETCCDI':'Celsius °C','tn10pETCCDI':'%',
              'tn90pETCCDI':'%','tx10pETCCDI':'%','tx90pETCCDI':'%','tnnETCCDI':'Celsius °C','tnxETCCDI':'Celsius °C','rx1dayETCCDI':'Millimeter','rx2dayETCCDI':'Millimeter','rx5dayETCCDI':'Millimeter',
              'txnETCCDI':'Celsius °C','txxETCCDI':'Celsius °C'}

InputDataDir =   # Change the path for saving the out file
OutputDataDir =    # Change the path for saving the out file
     
errorlist = []

def prep(ds):
    mod = ds.encoding["source"].split('_')[-4]
    ds = ds.assign_coords(mod=mod).expand_dims('mod')
    ssp = ds.encoding["source"].split('_')[-3][-6:]
    ds = ds.assign_coords(ssp=ssp).expand_dims('ssp')
    ds['time'] = pd.date_range('1950-01-01', periods=151, freq='AS-JAN')
    return ds

for var in tqdm(CMIP6_dic.keys()):

    flnm_CanESM_good = glob.glob(f'{InputDataDir}/Climdex/CanESM5*r1i1p*/{var}*nc')
    flnm_CanESM_all = glob.glob(f'{InputDataDir}/Climdex/CanESM5*/{var}*nc')
    all_fn = glob.glob(f'{InputDataDir}/Climdex/*/{var}*nc')
    flnm = set(all_fn) - set(flnm_CanESM_all) 
    nm = list(flnm)
    nm.extend(flnm_CanESM_good)
    
    data = None
    
    try:
        data = xr.open_mfdataset(flnm, preprocess=prep)
        data.index.to_pydatetime()
    except ValueError: # file or other error
        errorlist.append(f'{var}')
        
    if data is not None:
        sd = data[var].reduce(np.std, dim='time').rename('sd')
        mean = data[var].mean(dim='time').rename('mn')
        skew = data[var].reduce(stats.skew, dim='time').rename('skew')
        kurtosis = data[var].reduce(stats.kurtosis, dim='time').rename('kurtosis')
        IQR = data[var].reduce(stats.iqr, dim='time').rename('IQR')
        out = xr.merge([mean, sd, skew, kurtosis, IQR])
                                                                                            
        out.to_netcdf(f'{OutputDataDir}/stats/distribution_stats_{var}.nc')
        
        del([mean, sd, skew, kurtosis, IQR]) # free up some memory
        del([data, out])
        gc.collect()

print(errorlist)

    
#%%  Plotting

'''
import xarray as xr
import glob
import matplotlib.pyplot as plt

scenarios = ['ssp126','ssp245','ssp585'] # All emission scenarios

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']

variables = ['tasmax', 'tasmin', 'pr']

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5']

for mod in models:
        
    alldata = xr.open_mfdataset(glob.glob(f'{OutputDataDir}distribution_stats_{mod}*'))
    
    for var in variables:
        
        fig, axes = plt.subplots(figsize=(15,25))
        plt.axis('off')
        r,c = 5, 3
        i = 0
        
        for ssp in scenarios:
            
            data = alldata.sel(ssp=ssp, var=var)
            
            def setup(title):
                ax.set_xticklabels([])
                ax.set_yticklabels([])
                ax.set_xlabel("")
                ax.set_ylabel("")
                ax.set_title(title)
    
            ax = fig.add_subplot(r,c,1+i)
            data['mean'].plot(ax=ax)
            setup('Mean {ssp}')
    
            ax = fig.add_subplot(r,c,4+i)
            data['std'].plot(ax=ax)
            setup('Standard deviation')
            
            ax = fig.add_subplot(r,c,7+1)
            (data['std']/data['mean']).plot(ax=ax)
            setup('Coeff of variation')
    
            ax = fig.add_subplot(r,c,10+i)
            data.skew.plot(ax=ax)
            setup('Skew')
    
            ax = fig.add_subplot(r,c,13+i)
            data.kurtosis.plot(ax=ax)
            setup('Kurtosis')
    
          #  ax = fig.add_subplot(r,c,16+i)
          #  data.IQR.plot(ax=ax)
          #  setup('IQR')
          #  i = i + 1
    
        fig.suptitle(f'{mod} {var}', y=0.92, fontweight='bold')
        fig.savefig(f'{OutputDataDir}/{mod}_{var}_dist.jpg', bbox_inches='tight', dpi=120)
    '''            
#%%
'''
import xarray as xr
import glob
import matplotlib.pyplot as plt

variables = ['tasmin', 'tasmax', 'pr']
def prep(ds):
    mod = ds.encoding["source"].split('_')[2]
    ds = ds.assign_coords(mod=mod).expand_dims('mod')
    return ds

flnm = glob.glob(f'{OutputDataDir}*.nc')
alls = xr.open_mfdataset(flnm, preprocess=prep)

fig, ax = plt.subplots(figsize=(20,10))
fig.suptitle('Mean skew or kurtosis', y=0.92)
plt.axis('off')
r, c = 2, 3
i = 0
for var in variables:
    data = alls.sel(var=var)
    ax = fig.add_subplot(r,c,1+i)
    data.mean(dim=['ssp','mod']).skew.plot(ax=ax)
    ax = fig.add_subplot(r,c,4+i)
    data.mean(dim=['ssp','mod']).kurtosis.plot(ax=ax)
    i = i + 1
fig.savefig(f'{OutputDataDir}all_mod_mn_sk_kurt.jpg', bbox_inches='tight', dpi=120)


fig, ax = plt.subplots(figsize=(20,10))
fig.suptitle('Absolute maximum skew or kurtosis', y=0.92)
plt.axis('off')
r, c = 2, 3
i = 0
for var in variables:
    data = alls.sel(var=var)
    ax = fig.add_subplot(r,c,1+i)
    abs(data).max(dim=['ssp','mod']).skew.plot(ax=ax)
    ax = fig.add_subplot(r,c,4+i)
    abs(data).max(dim=['ssp','mod']).kurtosis.plot(ax=ax)
    i = i + 1

fig.savefig(f'{OutputDataDir}all_mod_abs_max_sk_kurt.jpg', bbox_inches='tight', dpi=120)

'''