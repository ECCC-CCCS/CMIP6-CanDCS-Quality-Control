#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 15:50:32 2022

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
import threddsclient


start_time = time.time()

emission = ['rcp26','rcp45','rcp85']

mods = ['BNU-ESM', 'CCSM4', 'CESM1-CAM5', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'FGOALS-g2', 'GFDL-CM3',
        'GFDL-ESM2G', 'GFDL-ESM2M', 'HadGEM2-AO',
        'HadGEM2-ES', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5', 'MPI-ESM-LR',
        'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M',
        'NorESM1-ME', 'bcc-csm1-1-m', 'bcc-csm1-1']   #CMIP5 models

models = ['ACCESS-CM2', 'ACCESS-ESM1-5', 'BCC-CSM2-MR', 'CMCC-ESM2', 'CNRM-CM6-1',
          'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg', 'EC-Earth3', 'FGOALS-g3', 'GFDL-ESM4',
          'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'KIOST-ESM',
          'MIROC-ES2L', 'MIROC6', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR', 'MRI-ESM2-0', 'NorESM2-LM', 
          'NorESM2-MM', 'TaiESM1', 'HadGEM3-GC31-LL', 'UKESM1-0-LL', 'KACE-1-0-G']     #CMIP6 models


#CMIP5_indices_max = ['tx_max', 'tn_max', 'cdd', 'rx1day']
#CMIP5_indices_min = ['tn_min', 'tx_min']
CMIP5_indices = ['tr_20', 'txgt_25', 'r10mm', 'r1mm', 'r20mm', 'frost_days', 'ice_days']

CMIP6_indices = {'prcptot':'prcptotETCCDI', 'tr_20':'trETCCDI_ann', 'txgt_25':'suETCCDI', 'r10mm':'r10mmETCCDI',
                 'r1mm':'r1mmETCCDI', 'r20mm':'r20mm', 'frost_days':'fdETCCDI', 'ice_days':'idETCCDI'}

CMIP5_dic = {'tr_20':'Tropical nights (days with tmin > 20C)','txgt_25':'Days with tmax > 25 (summer days)',
             'r10mm':'Wetdays > 10 mm','r1mm':'Wetdays > 1 mm','r20mm':'Wetdays > 20 mm','frost_days':'Days with tmin < 0 C',
             'ice_days':'Days with tmax < 0 C','tx_max':'Maximum of daily tmax','tn_max':'Maximum of daily tmin','cdd':'Maximum number of consecutive dry days (RR < 1 mm)',
             'rx1day':'Max 1 day precip amount','tn_min':'minimum of daily tmin','tx_min':'minimum of daily tmax'}

CMIP5_dic1 = {'tr_20':'Days','txgt_25':'Days',
              'r10mm':'Millimeter','r1mm':'Millimeter','r20mm':'Millimeter','frost_days':'Days',
              'ice_days':'Days','tx_max':'Celsius °C','tn_max':'Celsius °C','cdd':'Millimeter',
              'rx1day':'Millimeter','tn_min':'Celsius °C','tx_min':'Celsius °C'}

CMIP6_dic = {'cddETCCDI':'maximum number of consecutive days with PRCP < 1mm','csdiETCCDI':'Cold spell duration index',
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

CMIP6_dic1 = {'cddETCCDI':'Days','csdiETCCDI':'Days','cwdETCCDI':'Days','fdETCCDI':'Days','gslETCCDI':'Days','idETCCDI':'Days','prcptotETCCDI':'Millimeter',
              'r10mmETCCDI':'Days','r1mmETCCDI':'Days','r20mmETCCDI':'Days','r95daysETCCDI':'Days','r95pETCCDI':'Millimeter','r99daysETCCDI':'Days',
              'r99pETCCDI':'Millimeter','sdiiETCCDI':'Millimeter','su30ETCCDI':'Days','suETCCDI':'Days','wsdiETCCDI':'Days','trETCCDI_':'Days','dtrETCCDI':'Celsius °C','tn10pETCCDI':'%',
              'tn90pETCCDI':'%','tx10pETCCDI':'%','tx90pETCCDI':'%','tnnETCCDI':'Celsius °C','tnxETCCDI':'Celsius °C','rx1dayETCCDI':'Millimeter','rx2dayETCCDI':'Millimeter','rx5dayETCCDI':'Millimeter',
              'txnETCCDI':'Celsius °C','txxETCCDI':'Celsius °C'}

path = '/***REMOVED***/projects/CMIP6-BCCAQ-quality-control/***REMOVED***/CMIP5_CMIP6_Comparison/'    # Change the path for saving the file


for CMIP5_i in CMIP5_indices:
    for rcp in emission:
        my_list = []
        url = 'https://pavics.ouranos.ca/thredds/catalog/birdhouse/cccs_portal/indices/Final/BCCAQv2/'+CMIP5_i+'/YS/'+rcp+'/simulations/catalog.html'
        list_fld = []
        for m in mods:
            ncfiles = [ds for ds in threddsclient.crawl(url, depth=30) if m + '_' in ds.name]
            for n in ncfiles:
                if 'r1i1p1' in n.name:
                    list_fld.append(n)
        for model, nameM in zip(list_fld, mods):
            print(nameM)
            r = str(model.opendap_url())
            ds = xr.open_dataset(r, decode_times=False)
            ds['time'] = xr.decode_cf(ds).time
            indice_list_5 = list(ds.data_vars)
            indice_5 = indice_list_5[0]
            weights_5 = np.cos(np.deg2rad(ds.lat))
            new_data_5 = ds[indice_5].resample(time="AS").mean(dim="time")    # Change this for Max and Min indices 
            data_weighted_5 = new_data_5.weighted(weights_5)
            weighted_mean_5 = data_weighted_5.mean(dim=('lon', 'lat'))
            mean_5 = weighted_mean_5.to_dataset()
            if CMIP5_dic1[CMIP5_i] == 'Celsius °C':
              mean_5_C = mean_5 - 273.15
            else:
              mean_5_C = mean_5
            my_list.append(mean_5_C)
        ens_5 = ensembles.create_ensemble(my_list)
        xr.set_options(display_style="html", display_width=50)
        ens_perc_RCP = ensembles.ensemble_percentiles(ens_5, values=[10, 50, 90], split=False)
        if rcp == 'rcp26':
            s = 'ssp126'
        elif rcp == 'rcp45':
            s = 'ssp245'
        else:
            s = 'ssp585'
        ncfiles=[]
        CMIP6_i = CMIP6_indices[CMIP5_i] 
        for mod in models:
            print(m)
            if mod == 'CanESM5':
                Input = sorted(glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/'+mod+'_'+s+'_r10i1p2f1/' ))
            else:
                Input = sorted(glob.glob('/***REMOVED***/***REMOVED***/SD_PCIC/CMIP6_BCCAQv2_1/Climdex/'+mod+'_'+s+'_*/'))
            for folder in Input:
                file_list = os.listdir(folder)
                file_list.sort()
                matching = [f for f in file_list if CMIP6_i in f]
                for m in matching:
                    all_files = folder+m
                    ds = xr.open_dataset(all_files, decode_times=False)
                    ds['time'] = xr.decode_cf(ds).time
                    indice_list_6 = list(ds.data_vars)
                    indice_6 = indice_list_6[0]
                    weights_6 = np.cos(np.deg2rad(ds.lat))
                    new_data_6 = ds[indice_6].resample(time="AS").mean(dim="time")    # Change this for Max and Min indices
                    data_weighted_6 = new_data_6.weighted(weights_6)
                    weighted_mean_6 = data_weighted_6.mean(dim=('lon', 'lat'))
                    mean_6 = weighted_mean_6.to_dataset()
                    ncfiles.append(mean_6)
        ens_6 = ensembles.create_ensemble(ncfiles)    
        ens_perc_SSP = ensembles.ensemble_percentiles(ens_6, values=[10, 50, 90], split=False)
        plt.style.use("seaborn-dark")
        plt.rcParams["figure.figsize"] = (15, 7)
        fig, ax = plt.subplots(1)
        ax.fill_between(ens_perc_RCP.time.values, ens_perc_RCP[CMIP5_i].sel(percentiles=10).values,  
                        ens_perc_RCP[CMIP5_i].sel(percentiles=90).values, facecolor='#377eb8', alpha=0.4, label='CMIP5_'+rcp+"_10th to 90th percentile range")
        ax._get_lines.get_next_color()
        ax._get_lines.get_next_color()
        ax.plot(ens_perc_RCP.time.values, ens_perc_RCP[CMIP5_i].sel(percentiles=50).values, linewidth=3, color='#377eb8', label='CMIP5_'+rcp+"_Median")
        ax.fill_between(ens_perc_SSP.time.values, ens_perc_SSP[indice_6].sel(percentiles=10).values, 
                        ens_perc_SSP[indice_6].sel(percentiles=90).values, facecolor='#a65628', alpha=0.4, label='CMIP6_'+s+"_10th to 90th percentile range")
        ax._get_lines.get_next_color()
        ax._get_lines.get_next_color()
        ax.plot(ens_perc_SSP.time.values, ens_perc_SSP[indice_6].sel(percentiles=50).values, linewidth=3, color='#a65628', label='CMIP6_'+s+"_Median")
        ax.legend(loc='upper left')
        title1 = 'CMIP5_'+CMIP5_dic[CMIP5_i]+' vs. '+'CMIP6_'+CMIP6_dic[indice_6]
        y_axis1 = CMIP5_dic1[CMIP5_i]
        fig.suptitle(title1, fontsize=15)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel(y_axis1, fontsize=12)
        file_name = CMIP5_dic[CMIP5_i]+'_'+rcp+'_'+s+'CMIP5_CMIP6_Comparison.png'
        fig1 = plt.gcf()
        plt.show()
        fig1.savefig(path+file_name)
        new_ens_perc_tmean_p50 = ens_perc_SSP[indice_6].sel(percentiles=50).values - ens_perc_RCP[CMIP5_i].sel(percentiles=50).values
        fig, bx = plt.subplots(1)
        bx.plot(ens_perc_SSP.time.values, new_ens_perc_tmean_p50, linewidth=3, label='CMIP5/ CMIP6 Change_ Median')
        bx.legend(loc='upper left')
        title2 = 'Change for '+'CMIP6_'+CMIP6_dic[indice_6]+' / CMIP5_'+CMIP5_dic[CMIP5_i]
        y_axis2 = CMIP5_dic1[CMIP5_i]
        fig.suptitle(title2, fontsize=15)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel(y_axis2, fontsize=12)
        file_name1 = CMIP5_dic[CMIP5_i]+'_'+rcp+'_'+s+'_CMIP5_CMIP6_Change.png'
        fig2 = plt.gcf()
        plt.show()
        fig2.savefig(path+file_name1)
        plt.clf()
        plt.close('all')
print("It took", str(timedelta(seconds=time.time() - start_time)), "to run")
