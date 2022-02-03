# -*- coding: utf-8 -*-
import xarray as xr
import gc
from scipy import stats
import glob

rcps = ['rcp45'] # All emission scenarios

mods = ['BNU-ESM', 'CCSM4', 'CESM1-CAM5', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'FGOALS-g2', 'GFDL-CM3',
                'GFDL-ESM2G', 'GFDL-ESM2M', 'HadGEM2-AO',
                'HadGEM2-ES', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR',
                'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M'
            , 'NorESM1-ME', 'bcc-csm1-1-m', 'bcc-csm1-1']


variables = ['pr']

output = 'output path'
    

for mod in mods:
    print(mod)
    for var in variables:
        for r in rcps:
            inpath = "path to input folder"
            url= glob.glob(inpath+'new_BCCAQv2-'+r+'-'+var+'/*'+mod+'_'+'*207*nc' )
            data = xr.open_mfdataset(url)
            sd = data[var].std(dim='time').rename('std')
            mean = data[var].mean(dim='time').rename('mean')
            skew = data[var].reduce(stats.skew, dim='time').rename('skew')
            kurtosis = data[var].reduce(stats.kurtosis, dim='time').rename('kurtosis')
            IQR = data[var].reduce(stats.iqr, dim='time').rename('IQR')
            out = xr.merge([mean, sd, skew, kurtosis, IQR])
            out = out.assign_coords(rcp=r, var=var).expand_dims(['rcp', 'var'])   
                                                                                    
            out.to_netcdf(f'{output}/distribution_stats_{mod}_{var}_{r}.nc')
        
            del([mean, sd, skew, kurtosis, IQR]) # free up some memory
            del([data, out])
            gc.collect()
            
                
