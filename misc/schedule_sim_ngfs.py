#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 19 22:35:58 2020

@author: insauer
"""

import numpy as np
import pandas as pd
import sys
import os
import inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import argparse
from climada.entity.exposures.base import Exposures
from climada.entity.impact_funcs.river_flood import flood_imp_func_set
from climada.hazard.river_flood import RiverFlood
from climada.util.constants import RIVER_FLOOD_REGIONS_CSV
import copy


from climada.engine import Impact

parser = argparse.ArgumentParser(
    description='run climada for different climate and runoff models')
parser.add_argument(
    '--RF_model', type=str, default='H08',
    help='runoff model')
parser.add_argument(
    '--CL_model', type=str, default='princeton',
    help='Climate model')
parser.add_argument(
    '--scenario', type=str, default='rcp26',
    help='social interaction in ghms')

args = parser.parse_args()

# please set the path for the data containing the 
RF_PATH_FRC = '/p/projects/ebm/tobias_backup/floods/climada/isimip2a/flood_maps/fldfrc24_2.nc'


output = currentdir

# please set the directory containing the ISIMIP flood data here
flood_dir = '/p/projects/ebm/data/hazard/floods/isimip2b/'

if args.scenario == 'historical_1':
    years = np.arange(1861, 1931)
    filename = 'historical'
elif args.scenario == 'historical_2':
    years = np.arange(1931, 2006)
    filename = 'historical'
else:
    years = np.arange(2006, 2100)
    filename = args.scenario
#years = np.arange(1971, 2011)

# provide a file containing an income group given for each country ISO3 (optional)
country_info = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)
isos = country_info['ISO'].tolist()


cont_list = country_info['if_RF'].tolist()
l = (len(years) * (len(isos)-2))

PROT_STD = ['0', 'flopros', '100']

dataDF = pd.DataFrame(data={'Year': np.full(l, np.nan, dtype=int),
                            'Country': np.full(l, "", dtype=str),
                            'TotalAssetValue2005': np.full(l, np.nan, dtype=float),
                            'Impact_0': np.full(l, np.nan, dtype=float),
                            'Impact_Flopros': np.full(l, np.nan, dtype=float),
                            'Impact_100': np.full(l, np.nan, dtype=float),
                            'Impact_0_2y': np.full(l, np.nan, dtype=float),
                            'Impact_Flopros_2y': np.full(l, np.nan, dtype=float),
                            'Impact_100_2y': np.full(l, np.nan, dtype=float),
                            })
# set JRC impact functions
if_set = flood_imp_func_set()

fail_lc = 0
line_counter = 0

# loop over all countries
for cnt_ind in range(len(isos)):
    country = [isos[cnt_ind]]
    
    if country[0] in ['GIB','MCO']:
        continue

    # setting fixed exposures
    
    ngfs_exp = Exposures()
    ngfs_exp.read_hdf5('/p/projects/ebm/inga/ngfs/data/exp_ngfs_{}.h5'.format(country[0]))
    #gdpaFix.correct_for_SSP(ssp_corr, country[0])
    save_lc = line_counter
    
    # loop over protection standards
    for pro_std in range(len(PROT_STD)):
        line_counter = save_lc
        dph_path = flood_dir + '{}/{}/{}/depth-150arcsec/flddph_annual_max_gev_0.1mmpd_protection-{}.nc'\
            .format(args.CL_model, args.RF_model, filename, PROT_STD[pro_std])
        frc_path = flood_dir + '{}/{}/{}/area-150arcsec/fldfrc_annual_max_gev_0.1mmpd_protection-{}.nc'\
            .format(args.CL_model, args.RF_model, filename, PROT_STD[pro_std])
            
        if not os.path.exists(dph_path):
            print('{} path not found'.format(dph_path))
            break
        if not os.path.exists(frc_path):
            print('{} path not found'.format(frc_path))
            break

        # set flood hazard
        rf = RiverFlood()
        
        rf.set_from_nc(dph_path=dph_path, frc_path=frc_path,
                       countries=country, years = years, ISINatIDGrid=True)
        # set flood hazard for subregions

        rf2y = copy.copy(rf)
        
        rf2y.exclude_returnlevel(RF_PATH_FRC)

        # loop over all years
        for year in range(len(years)):
            print('country_{}_year{}_protStd_{}'.format(country[0], str(years[year]), PROT_STD[pro_std]))
            ini_date = str(years[year]) + '-01-01'
            fin_date = str(years[year]) + '-12-31'
            dataDF.iloc[line_counter, 0] = years[year]
            dataDF.iloc[line_counter, 1] = country[0]

            #gdpa.correct_for_SSP(ssp_corr, country[0])
            # calculate damages for all combinations
            imp_fl=Impact()
            imp_fl.calc(ngfs_exp, if_set, rf2y.select(date=(ini_date,fin_date)))
            imp2y_fl=Impact()
            imp2y_fl.calc(ngfs_exp, if_set, rf2y.select(date=(ini_date,fin_date)))
            
            # write dataframe
            dataDF.iloc[line_counter, 2] = imp_fl.tot_value
            dataDF.iloc[line_counter, 3 + pro_std] = imp_fl.at_event[0]
            dataDF.iloc[line_counter, 6 + pro_std] = imp2y_fl.at_event[0]

            line_counter+=1
   
    # save output dataframe
    dataDF.to_csv('/p/projects/ebm/inga/ngfs/results/damage_fixexp2005_{}_{}_{}.csv'.format(args.CL_model, args.RF_model, args.scenario))


