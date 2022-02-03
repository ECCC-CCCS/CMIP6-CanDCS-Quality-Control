#%%

import xarray as xr
import glob
import matplotlib.pyplot as plt

inputpath = 'input folder path' 
output = 'output folder path' 

variables = ['pr']
def prep(ds):
    mod = ds.encoding["source"].split('_')[2]
    ds = ds.assign_coords(mod=mod).expand_dims('mod')
    return ds

flnm = glob.glob(inputpath+'/*.nc')
alls = xr.open_mfdataset(flnm, preprocess=prep)

fig, ax = plt.subplots(figsize=(20,10))
fig.suptitle('Mean skew or kurtosis', y=0.92)
plt.axis('off')
r, c = 2, 3
i = 0
for var in variables:
    data = alls.sel(var=var)
    ax = fig.add_subplot(r,c,1+i)
    data.mean(dim=['rcp','mod']).skew.plot(ax=ax)
    ax = fig.add_subplot(r,c,4+i)
    data.mean(dim=['rcp','mod']).kurtosis.plot(ax=ax)
    i = i + 1
fig.savefig(f'{output}all_mod_mn_sk_kurt.jpg', bbox_inches='tight', dpi=120)


fig, ax = plt.subplots(figsize=(20,10))
fig.suptitle('Absolute maximum skew or kurtosis', y=0.92)
plt.axis('off')
r, c = 2, 3
i = 0
for var in variables:
    data = alls.sel(var=var)
    ax = fig.add_subplot(r,c,1+i)
    abs(data).max(dim=['rcp','mod']).skew.plot(ax=ax)
    ax = fig.add_subplot(r,c,4+i)
    abs(data).max(dim=['rcp','mod']).kurtosis.plot(ax=ax)
    i = i + 1

fig.savefig(f'{output}all_mod_abs_max_sk_kurt.jpg', bbox_inches='tight', dpi=120)

