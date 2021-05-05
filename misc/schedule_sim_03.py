#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 19 22:35:58 2020

@author: insauer
"""

#!/usr/bin/env python
import numpy as np
import pandas as pd
import sys
import os
import inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import argparse
import geopandas as gpd
from climada.entity.exposures.base import Exposures
from climada.entity.impact_funcs.river_flood import flood_imp_func_set
from climada.hazard.river_flood import RiverFlood
from climada.util.constants import RIVER_FLOOD_REGIONS_CSV
from shapely.geometry.multipolygon import MultiPolygon
import copy
from climada.hazard.centroids import Centroids
from climada.engine import Impact
from osgeo import gdal
from climada.util import coordinates as u_coord
print('parse')

parser = argparse.ArgumentParser(
    description='run climada for different climate and runoff models')
parser.add_argument(
    '--RF_model', type=str, default='H08',
    help='runoff model')
parser.add_argument(
    '--CL_model', type=str, default='princeton',
    help='Climate forcing dataset')


args = parser.parse_args()

gdp_path = '/p/projects/ebm/inga/climada_exposures/asset/'

print('start')
iso3_list=['ITA', 'GHA', 'MRT', 'MTQ', 'TJK', 'COL', 'MMR',
          'VEN', 'YEM', 'MEX', 'UKR', 'SWZ', 'PYF', 'TON', 'BIH',
          'CPV', 'CYP', 'BTN', 'NFK', 'BMU', 'MWI', 'KGZ', 'SLE',
          'GAB', 'PSE', 'PNG', 'GUF', 'EGY', 'ASM', 'ARM', 'CHL',
          'LTU', 'TLS', 'SCG', 'GRC', 'KNA', 'PRY', 'HKG', 'VNM',
          'NIU', 'NER', 'NZL', 'COM', 'CYM', 'BGR', 'BOL', 'BRN',
          'CAN', 'BEN', 'AUT', 'KAZ', 'FLK', 'PER', 'STP', 'KOR',
          'IRN', 'PAK', 'IDN', 'FSM', 'ATG', 'BFA', 'NGA', 'MAC',
          'ZMB', 'AND', 'SPM', 'SHN', 'FIN', 'ISR', 'GUM', 'HUN',
          'GTM', 'DOM', 'POL', 'THA', 'ZWE', 'AIA', 'BRA', 'WLF',
          'LIE', 'BGD', 'PAN', 'KWT', 'SLV', 'KIR', 'GIN', 'JAM',
          'SYR', 'ARG', 'TZA', 'BWA', 'NIC', 'LKA', 'MKD', 'COG',
          'MDG', 'BLZ', 'AZE', 'PHL', 'HRV', 'PRK', 'SAU', 'REU',
          'JOR', 'SGP', 'DJI', 'PCN', 'SMR', 'AFG', 'UGA', 'TCA',
          'MOZ', 'MYS', 'DEU', 'EST', 'PRT', 'QAT', 'KEN', 'IMN',
          'CHE', 'ALB', 'URY', 'USA', 'LUX', 'COD', 'VGB', 'DNK',
          'RUS', 'SVN', 'KHM', 'GLP', 'IRL', 'ESP', 'FRO', 'ISL',
          'SOM', 'ECU', 'SUR', 'LCA', 'TCD', 'MNG', 'LBY', 'SDN',
          'ERI', 'PLW', 'JPN', 'ROU', 'GBR', 'GEO', 'PRI', 'SEN',
          'SJM', 'HTI', 'CAF', 'NLD', 'CUB', 'MDA', 'ZAF', 'UZB',
          'FJI', 'ETH', 'SVK', 'IRQ', 'GNQ', 'CZE', 'ARE', 'LVA',
          'BRB', 'GNB', 'DMA', 'FRA', 'DZA', 'RWA', 'SYC', 'BDI',
          'MLT', 'NRU', 'AUS', 'MNP', 'LBN', 'MCO', 'MSR', 'TKM',
          'ANT', 'GMB', 'TTO', 'LBR', 'CMR', 'LSO', 'OMN', 'BHR',
          'MUS', 'NCL', 'MYT', 'ABW', 'WSM', 'MDV', 'JEY', 'GRD',
          'GUY', 'IND', 'HND', 'VIR', 'TUV', 'NPL', 'AGO', 'NOR',
          'BLR', 'LAO', 'BEL', 'BHS', 'TGO', 'TUN', 'MLI', 'SLB',
          'GGY', 'GIB', 'SWE', 'TUR', 'COK', 'CRI', 'TWN', 'MHL',
          'NAM', 'VCT', 'VUT', 'CHN', 'MAR', 'CIV', 'TKL']

year = 2000
gdpa = Exposures()
gdpa.read_hdf5(gdp_path + 'asset_{}_{}.h5'.format(iso3_list[0], str(year)))

for n,iso in enumerate(iso3_list[1:]):
    print(iso)
    if (iso == 'MCO') or (iso=='GIB'):
        continue
    gdpa1 = Exposures()
    gdpa1.read_hdf5(gdp_path + 'asset_{}_{}.h5'.format(iso, str(year)))
    
    gdpa = gdpa.append(gdpa1)

gdpa.write_hdf5('/home/insauer/data/plot_exposure/global_exposure_2000.h5')
raster, meta = u_coord.points_to_raster(gdpa, ['value'], res=None, raster_res=None)
print('done')
#tifffile = "/home/insauer/data/plot_exposure/global_exposure_2000.tiff"
#u_coord.write_raster(tifffile, raster, meta)

#netcdffile = "/home/insauer/data/plot_exposure/global_exposure_2000.nc"

# #Do not change this line, the following command will convert the geoTIFF to a netCDF
#gdal.Translate(netcdffile, tifffile, format='NetCDF')
