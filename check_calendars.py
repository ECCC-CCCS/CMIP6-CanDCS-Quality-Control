#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 19 22:13:44 2021

For each model and var in the PCIC CMIP6 ensemble, open one 5 year file, extract the calendar and ensure that 
it has the correct number of days
  
"""


import xarray as xr
import xclim
import os, glob



path = 'input path'

#switch to their directory
os.chdir(path)
#get all files in CRD folder
flds = os.listdir()
#remove extra files
flds.remove('PCIC_SD_download.sh')
flds.remove('Climdex')

inds = ['tasmax', 'tasmin', 'pr']
ssps = ['ssp126', 'ssp245', 'ssp585']


for fld in flds:
    #switch into that folder
    os.chdir(path+fld+'/')
    #glob one file and check its calendar
    beg = glob.glob('*1950*')
    cal = xclim.core.calendar.get_calendar(xr.open_dataset(beg[0]))
    #for each file make sure, there are the right number of timesteps
    for fl in os.listdir():
        if ".nc" not in fl:
            pass
        else:   
            dsl =  len(xr.open_dataset(fl).time)
            if cal == 'default':
               if dsl == 2191 or dsl == 2192 or dsl == 1826 or dsl == 1827 :
                   pass
               else:
                   print('Missing '+fl)
            elif cal == 'noleap':
                if dsl == 2190 or dsl == 1825:
                    pass
                else:
                    print('Missing '+fl)
            else:
               if dsl == 2160 or dsl == 1800:
                   pass
               else:
                   print('Missing '+fl)
               
               

               
    
   
    
            
            
            
            
            
            



