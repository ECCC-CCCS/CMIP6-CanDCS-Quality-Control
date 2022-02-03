"""
Created on Wed Jan 12 16:54:27 2022

@author: Housseyni SANKARE
"""
import time
import pandas as pd
import xarray as xr
import glob, os
import numpy as np

var="relative_humidity"
input_dir="/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/"
output_dir="/***REMOVED***/projects/CMIP6-BCCAQ-quality-control/level_of_warming/"
########################################################################################################################
########## read table of levels warming                    ############################################################
scenarios=["ssp585","ssp245","ssp126"]
for sc in scenarios:
    lw_ssp = pd.read_csv(output_dir+'global_cmip6_'+sc+'_tas_degree_anomaly_years_26_GCMs.csv')
    models=lw_ssp["Model"]
    run=lw_ssp["Run"]
    year_1d=lw_ssp["1_Degree"]
    year_2d=lw_ssp["2_Degrees"]
    year_3d=lw_ssp["3_Degrees"]
    year_4d=lw_ssp["4_Degrees"]
##########################################################################################################################
############read cmip6 data (Climdex) and find  time of degrre warming corresponding#########################################################
    for ii in range(0,len(models)): # loop on models
        data_dir=input_dir+models[ii]+"_"+sc+"_"+run[ii]
        output_dir1=output_dir+models[ii]+"_"+sc+"_"+run[ii]
        # Check if the path exists or not in the output dir
        isExist = os.path.exists(output_dir1)
        if not isExist:
        # Create a new directory because it does not exist 
            os.makedirs(output_dir1)
            print("The new directory is created!")
        os.chdir(data_dir)    
        ann_files=glob.glob('*_ann'+'*.nc')
        for jj in range(0,len(ann_files)): # loop on the variables
             ds = xr.open_dataset(data_dir+"/"+ann_files[jj], decode_times=False)
             ds['time'] = xr.decode_cf(ds).time
             #il y a des annee ou un degre n est pas atteint eviter ces annee

             #JF comment: for following, we need to develop 30 year climatologies for each variable, around the particular year, and not just extract the map for that particular year.  See below comment.

             y1d=year_1d[ii]
             if np.math.isnan(y1d):
                 yeari1=2020
                 ds1d=ds.sel(time=ds.time.dt.year.isin(yeari1))
                 var=list(ds1d.variables.keys())[3]
                 ds1d.variables[var].values[:,:]=np.nan
             else:
                  #yeari1=(ds.time.dt.year==year_1d[ii])
                  yeari1=np.arange(y1d-15,y1d+15) # 30 years mobile
                  ds1d =ds.sel(time=ds.time.dt.year.isin(yeari1))#JF: Here is where we need to extract a 3D (x,y,t) climatology, for thirty years of data surrounding the year_1d[ii] value.  Then take a time average of this to obtain the 2D (x,y) field to assign to the ds1d variable, below.
             y2d=year_2d[ii]
             if np.math.isnan(y2d):
                 yeari2=2020
                 ds2d=ds.sel(time=ds.time.dt.year.isin(yeari2))
                 var=list(ds2d.variables.keys())[3]
                 ds2d.variables[var].values[:,:]=np.nan
             else:
                  #yeari2=(ds.time.dt.year==year_2d[ii])
                  yeari2=np.arange(y2d-15,y2d+15)
                  ds2d =ds.sel(time=ds.time.dt.year.isin(yeari2))
             y3d=year_3d[ii]
             if np.math.isnan(y3d):
                 yeari3=2020
                 ds3d=ds.sel(time=ds.time.dt.year.isin(yeari3))
                 var=list(ds3d.variables.keys())[3]
                 ds3d.variables[var].values[:,:]=np.nan
             else:
                  #yeari3=(ds.time.dt.year==year_3d[ii])
                  yeari3=np.arange(y3d-15,y3d+15)
                  ds3d =ds.sel(time=ds.time.dt.year.isin(yeari3))   
             y4d=year_4d[ii]
             if (np.math.isnan(y4d)):
                 yeari4=2020# a random year, just to keep the right dim
                 ds4d=ds.sel(time=ds.time.dt.year.isin(yeari4)) 
                 var=list(ds4d.variables.keys())[3]
                 ds4d.variables[var].values[:,:]=np.nan
             else:
                 # yeari4=(ds.time.dt.year==year_4d[ii])
                  yeari4=np.arange(y4d-15,y4d+15)
                  ds4d =ds.sel(time=ds.time.dt.year.isin(yeari4))     
                  
             ### climatological mean calculation
             ds1dmean=ds1d.mean("time")
             ds2dmean=ds2d.mean("time")
             ds3dmean=ds3d.mean("time")
             ds4dmean=ds4d.mean("time")
             lev_warming=np.arange(1,5)
             dsf=xr.concat([ds1dmean,ds2dmean,ds3dmean,ds4dmean],xr.DataArray(lev_warming, name='lev_warm', dims=['dT']))
             dsf.to_netcdf(output_dir1+"/"+ann_files[jj][:-12]+"lev_warm.nc")      
            



