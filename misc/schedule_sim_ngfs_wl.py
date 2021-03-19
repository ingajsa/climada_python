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
import matplotlib
matplotlib.use('Agg')
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
import argparse
from climada.entity.exposures.base import Exposures
from climada.entity.impact_funcs.river_flood import flood_imp_func_set
from climada.hazard.river_flood import RiverFlood
from climada.util.constants import RIVER_FLOOD_REGIONS_CSV
import matplotlib.pyplot as plt
import copy
from osgeo import gdal
from climada.util import coordinates as u_coord


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
parser.add_argument(
    '--WL', type=float, default=1.1,
    help='warming level')
parser.add_argument(
    '--start_year', type=int, default=2006,
    help='start year')
parser.add_argument(
    '--stop_year', type=int, default=2099,
    help='social interaction in ghms')


args = parser.parse_args()

# please set the path for the data containing the 
RF_PATH_FRC = '/p/projects/ebm/tobias_backup/floods/climada/isimip2a/flood_maps/fldfrc24_2.nc'


output = currentdir

# please set the directory containing the ISIMIP flood data here
flood_dir = '/p/projects/ebm/data/hazard/floods/isimip2b/'


years = np.arange(args.start_year, args.stop_year+1)

filename = args.scenario


# provide a file containing an income group given for each country ISO3 (optional)
country_info = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)
isos = country_info['ISO'].tolist()


cont_list = country_info['if_RF'].tolist()
lines = (len(years) * (len(isos)-2))

PROT_STD = ['0', 'flopros', '100']

dataDF = pd.DataFrame(data={'Year': np.full(lines, np.nan, dtype=int),
                            'Country': np.full(lines, "", dtype=str),
                            'TotalAssetValue2005': np.full(lines, np.nan, dtype=float),
                            'Impact_Flopros': np.full(lines, np.nan, dtype=float),
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

    dph_path = flood_dir + '{}/{}/{}/depth-150arcsec/flddph_annual_max_gev_0.1mmpd_protection-{}.nc'\
        .format(args.CL_model, args.RF_model, filename, PROT_STD[1])
    frc_path = flood_dir + '{}/{}/{}/area-150arcsec/fldfrc_annual_max_gev_0.1mmpd_protection-{}.nc'\
        .format(args.CL_model, args.RF_model, filename, PROT_STD[1])
        
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

    imp_fl=Impact()
    imp_fl.calc(ngfs_exp, if_set, rf)
    
    # write dataframe
    dataDF['Year'].iloc[line_counter: line_counter + len(years)] = years
    dataDF['Country'].iloc[line_counter: line_counter + len(years)] = country[0]
    dataDF['TotalAssetValue2005'].iloc[line_counter:line_counter + len(years)] = imp_fl.tot_value
    dataDF['Impact_Flopros'].iloc[line_counter:line_counter + len(years)] = imp_fl.at_event
    line_counter = line_counter + len(years)
    # save output dataframe
    imp_fl.write_csv('/p/projects/ebm/inga/ngfs/results/impact_files_wl_{}/impact_{}_{}_{}_{}_{}.csv'.format(str(args.WL),country[0], args.CL_model, args.RF_model, args.scenario, str(args.WL)))

iso3 = isos[0]
imp = Impact()
imp.read_csv('/p/projects/ebm/inga/ngfs/results/impact_files_wl_' + str(args.WL) + '/' + "impact_" + iso3 + "_" + args.CL_model + "_" + args.RF_model + "_" + args.scenario + ".csv")
exp = imp._build_exp()
for iso3 in isos[1:]:
    if iso3 == "GIB":
        continue
    if iso3 == "MCO":
        continue
    imp = Impact()
    imp.read_csv('/p/projects/ebm/inga/ngfs/results/impact_files_wl_' + str(args.WL) + '/' + "impact_" + iso3 + "_" + args.CL_model + "_" + args.RF_model + "_" + args.scenario + ".csv")
    exp_tmp = imp._build_exp()
    exp = exp.append(exp_tmp, ignore_index=True)
    
tifffile = "/p/projects/ebm/inga/ngfs/results/impact_files_wl_" + str(args.WL) + "/global_impact_" + iso3 + "_" + args.CL_model + "_" + args.RF_model + "_" + args.scenario +".tiff"
print(tifffile)
raster, meta = u_coord.points_to_raster(exp, ['value'], res=None, raster_res=None, scheduler='threads')
u_coord.write_raster(tifffile, raster, meta)

netcdffile = "/p/projects/ebm/inga/ngfs/results/impact_files_wl_" + str(args.WL) + "/global_impact_" + iso3 + "_" + args.CL_model + "_" + args.RF_model + "_" + args.scenario + ".nc"

print(netcdffile)
#Do not change this line, the following command will convert the geoTIFF to a netCDF
gdal.Translate(netcdffile, tifffile, format='NetCDF')