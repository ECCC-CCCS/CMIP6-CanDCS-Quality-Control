# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 15:20:02 2022

@author: VanVlietL
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 16:00:34 2022

@author: VanVlietL
"""
import xarray as xr
import glob
from tqdm import tqdm
import matplotlib.pyplot as plt
import cftime

scenarios = ['ssp126','ssp245','ssp585'] # All emission scenarios

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']

variables = ['tasmax', 'tasmin', 'pr']

dates = ['2050-01-01', '2050-07-01']

#InputDataDir
#OutputDataDir 

def prep(ds):
   ssp = ds.encoding["source"].split('+')[-1][:6]
   ds = ds.assign_coords(ssp=ssp).expand_dims('ssp')   
   return ds

for var in tqdm(variables):
    for mod in tqdm(models):
        
        if mod == 'CanESM5': 
            flnm = glob.glob(f'{InputDataDir}{mod}_{var}*r1i1*/*2050*nc')
        else:
            flnm = glob.glob(f'{InputDataDir}{mod}_{var}*/*2050*nc')
        data = xr.open_mfdataset(flnm, preprocess=prep, use_cftime=True)[var]
        fig, ax = plt.subplots(figsize=(20,20))
        plt.axis('off')
        i = 1
        for ssp in scenarios[:-1]:
            for tt in dates:
                ax = fig.add_subplot(3,2,i)
                im = ax.pcolormesh(data.lon, data.lat, data.sel(ssp=ssp, time=tt).squeeze(), shading='auto')
                fig.colorbar(im)
                plt.axis('off')
                ax.set_title(f'{ssp} {tt}')
                i = i + 1
                plt.show()
        fig.suptitle(f'{mod} {var}', y=0.93)
                fig.savefig(f'{OutputDataDir}/time_slice_figs_{mod}_{var}.jpg', bbox_inches='tight', dpi=90)
     