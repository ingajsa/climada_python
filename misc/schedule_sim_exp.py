#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 15:30:49 2021

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
from climada.entity.exposures.gdp_asset import GDP2Asset
from climada.util.constants import RIVER_FLOOD_REGIONS_CSV

parser = argparse.ArgumentParser(
    description='run climada for different climate and runoff models')
parser.add_argument(
    '--RF_model', type=str, default='H08',
    help='runoff model')
parser.add_argument(
    '--CL_model', type=str, default='princeton',
    help='Climate forcing dataset')
parser.add_argument(
    '--cnt0', type=int, default=0,
    help='runoff model')
parser.add_argument(
    '--cnt1', type=int, default=13,
    help='Climate forcing dataset')
parser.add_argument(
    '--n_bas', type=int, default=100,
    help='Climate forcing dataset')


args = parser.parse_args()


# please set the path for the exposure data here (spatially explicit GDP data)
gdp_path = '/p/projects/ebm/data/exposure/gdp/processed_data/gdp_1850-2100_downscaled-by-nightlight_2.5arcmin_remapcon_new_yearly_shifted.nc'
pop_path = '/p/projects/ebm/data/exposure/population/hyde_ssp2_1860-2015_0150as_yearly_zip.nc4'

ssp_resc = '/home/insauer/data/exposure_rescaling/resc_ssp_transition.csv'
cap_stock_conv = '/home/insauer/data/exposure_rescaling/totalwealth_capital_stock_rescaling.csv'


years = np.arange(1971, 2011)

country_info = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)
isos = country_info['ISO'].tolist()[args.cnt0:args.cnt1]

# loop over all countries
for c, cnt_iso in enumerate(isos):

    if cnt_iso in ['GIB', 'MCO']:
        continue

    for y, year in enumerate(years):
        print('country_{}_year{}'.format(cnt_iso, year))

        gdpa = GDP2Asset()
        gdpa.set_countries(countries=[cnt_iso], ref_year=year, path=gdp_path, ssp_resc_path=ssp_resc,
           cap_resc_path=cap_stock_conv)
        gdpa.write_hdf5('/p/projects/ebm/inga/climada_exposures/population/pop_{}_{}.h5'.format(cnt_iso, str(year)))
        
        
        pop = GDP2Asset()
        pop.set_countries(countries=[cnt_iso], ref_year=year, path=pop_path, unit='pop')
        pop['if_RF'] = 7
        gdpa.write_hdf5('/p/projects/ebm/inga/climada_exposures/asset/asset_{}_{}.h5'.format(cnt_iso, str(year)))
