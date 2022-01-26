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
import netcdftime

scenarios = ['ssp126','ssp245','ssp585'] # All emission scenarios

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']
models = ['EC-Earth3-Veg', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']

variables = ['tasmax', 'tasmin', 'pr']

#InputDataDir
#OutputDataDir 
    
errorlist = []

for mod in tqdm(models):
    for var in tqdm(variables):
        for ssp in scenarios:
            if mod == 'CanESM5':
                flnm = glob.glob(f'{InputDataDir}{mod}_{var}*r1i1*{ssp}*/*nc')
            else:
                flnm = glob.glob(f'{InputDataDir}{mod}_{var}*{ssp}*/*nc' )
            
            data = None
            
            try:
                data = xr.open_mfdataset(flnm).sel(time=slice('2070-01-01','2079-12-30'))
            except TypeError: # for 360 day calendars
                data = xr.open_mfdataset(flnm).sel(time=slice(netcdftime._netcdftime.Datetime360Day(2070,1,1,12),
                                                              netcdftime._netcdftime.Datetime360Day(2079,12,31,12)))
            except ValueError: # file or other error
                errorlist.append(f'{mod} {var} {ssp}')
                
            if data is not None:
                sd = data[var].std(dim='time').rename('std')
                mean = data[var].mean(dim='time').rename('mean')
                skew = data[var].reduce(stats.skew, dim='time').rename('skew')
                kurtosis = data[var].reduce(stats.kurtosis, dim='time').rename('kurtosis')
                IQR = data[var].reduce(stats.iqr, dim='time').rename('IQR')
                out = xr.merge([mean, sd, skew, kurtosis, IQR])
                out = out.assign_coords(ssp=ssp, var=var).expand_dims(['ssp', 'var'])   
                                                                                        
                out.to_netcdf(f'{OutputDataDir}/distribution_stats_{mod}_{var}_{ssp}.nc')
                
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