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


from climada.engine import Impact

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


PROT_STD = ['0','flopros']

# please set the path for the exposure data here (spatially explicit GDP data)
gdp_path = '/p/projects/ebm/inga/climada_exposures/asset/'
pop_path = '/p/projects/ebm/inga/climada_exposures/population/'

basin_country_link = pd.read_csv('/home/insauer/data/river_basins/HYDROSHED_3_basin_country_link.csv')
# please set the path for the data containing the

path = '/home/insauer/data/river_basins/standard/'
conts = ['af', 'ar', 'as', 'au', 'eu', 'gr', 'na', 'sa', 'si']
gf = gpd.GeoDataFrame()

for cont in conts:
    
    gdf = gpd.read_file(path+'{}/hybas_{}_lev03_v1c/hybas_{}_lev03_v1c.shp'.format(cont, cont, cont))
    gf = gf.append(gdf)

#basins = gf['HYBAS_ID'].tolist()

flood_dir = '/p/projects/ebm/data/hazard/floods/isimip2a/'

if args.CL_model == 'watch':
    years = np.arange(1971, 2002)
else:
    years = np.arange(1971, 2011)

country_info = pd.read_csv(RIVER_FLOOD_REGIONS_CSV)
isos = country_info['ISO'].tolist()[args.cnt0:args.cnt1]


cont_list = country_info['if_RF'].tolist()[args.cnt0:args.cnt1]
l = (len(years) * args.n_bas)
continent_names = ['Africa', 'Asia', 'Europe', 'NorthAmerica', 'Oceania', 'SouthAmerica']


dataDF = pd.DataFrame(data={'Year': np.full(l, np.nan, dtype=int),
                            'Country': np.full(l, "", dtype=str),
                            'Basin': np.full(l, np.nan, dtype=int),
                            'Region': np.full(l, "", dtype=str),
                            'Continent': np.full(l, "", dtype=str),
                            'total_asset': np.full(l, np.nan, dtype=float),
                            'total_pop': np.full(l, np.nan, dtype=float),
                            'mean_flddph_0': np.full(l, np.nan, dtype=float),
                            'mean_flddph_flopros': np.full(l, np.nan, dtype=float),
                            'fldvol_0': np.full(l, np.nan, dtype=float),
                            'fldvol_flopros': np.full(l, np.nan, dtype=float),
                            'exppop_0': np.full(l, np.nan, dtype=float),
                            'exppop_flopros': np.full(l, np.nan, dtype=float),
                            'D_ExpAssCliExp_0': np.full(l, np.nan, dtype=float),
                            'D_ExpAssCliExp_flopros': np.full(l, np.nan, dtype=float),
                            'D_ExpAss1980_0': np.full(l, np.nan, dtype=float),
                            'D_ExpAss1980_flopros': np.full(l, np.nan, dtype=float),
                            'D_ExpAss2010_0': np.full(l, np.nan, dtype=float),
                            'D_ExpAss2010_flopros': np.full(l, np.nan, dtype=float),
                            'D_CliExp_0': np.full(l, np.nan, dtype=float),
                            'D_CliExp_flopros': np.full(l, np.nan, dtype=float),
                            'D_1980_0': np.full(l, np.nan, dtype=float),
                            'D_1980_flopros': np.full(l, np.nan, dtype=float),
                            'D_2010_0': np.full(l, np.nan, dtype=float),
                            'D_2010_flopros': np.full(l, np.nan, dtype=float)
                            })
# set JRC impact functions
if_set = flood_imp_func_set()

fail_lc = 0
line_counter = 0

# loop over all countries
for c, cnt_iso in enumerate(isos):
    cnt_basins = basin_country_link.loc[basin_country_link[cnt_iso]==1, 'BASIN']
    cnt_basins.tolist()

    if cnt_iso in ['GIB', 'MCO']:
        continue
    reg = country_info.loc[country_info['ISO'] == cnt_iso, 'Reg_name'].values[0]
    conts = country_info.loc[country_info['ISO'] == cnt_iso, 'if_RF'].values[0]
    cont = continent_names[int(conts-1)]
    
    # setting fixed exposures
    gdpa1980 = Exposures()
    gdpa1980.read_hdf5(gdp_path + 'asset_{}_1980.h5'.format(cnt_iso))
    gdpa2010 = Exposures()
    gdpa2010.read_hdf5(gdp_path + 'asset_{}_2010.h5'.format(cnt_iso))

    save_lc = line_counter
    
    # loop over protection standards
    for pro_std in PROT_STD:
        line_counter = save_lc
        dph_path = flood_dir + '{}/{}/depth-150arcsec/flddph_annual_max_gev_0.1mmpd_protection-{}.nc'\
            .format(args.CL_model, args.RF_model, pro_std)
        frc_path = flood_dir + '{}/{}/area-150arcsec/fldfrc_annual_max_gev_0.1mmpd_protection-{}.nc'\
            .format(args.CL_model, args.RF_model, pro_std)

        if not os.path.exists(dph_path):
            print('{} path not found'.format(dph_path))
            break
        if not os.path.exists(frc_path):
            print('{} path not found'.format(frc_path))
            break

        for i, bas_id in enumerate(cnt_basins):
            basin_shp = gf.loc[gf['HYBAS_ID'] == bas_id, 'geometry'].values[0]
            rf = RiverFlood()
            try:
                rf.set_from_nc(shape=basin_shp, years=years, dph_path=dph_path, frc_path=frc_path)
            except TypeError:
                basin_shp = MultiPolygon([basin_shp])
                rf.set_from_nc(shape=basin_shp, years=years, dph_path=dph_path, frc_path=frc_path)
            
            rf.set_flooded_area(save_centr=True)
            rf.set_flood_volume()

            for y, year in enumerate(years):
                print('country_{}_year{}_protStd_{}'.format(cnt_iso, year, pro_std))
                ini_date = str(year) + '-01-01'
                fin_date = str(year) + '-12-31'
                dataDF['Year'].iloc[line_counter] = year
                dataDF['Country'].iloc[line_counter] = cnt_iso
                dataDF['Basin'].iloc[line_counter] = bas_id
                dataDF['Region'].iloc[line_counter] = reg
                dataDF['Continent'].iloc[line_counter] = cont
                dataDF['fldvol_{}'.format(pro_std)].iloc[line_counter] = rf.fv_annual[y,0]
                depth = rf.intensity.todense()[y]
                dataDF['mean_flddph_{}'.format(pro_std)].iloc[line_counter] = depth[np.where(depth>0)].mean()
                # set variable exposure
                pop = Exposures()
                pop.read_hdf5(pop_path + 'pop_{}_{}.h5'.format(cnt_iso, str(year)))
                pop['if_RF'] = 7
                #gdpa.correct_for_SSP(ssp_corr, country[0])
                # calculate damages for all combinations
                exp_pop = Impact()
                exp_pop.calc(pop, if_set, rf.select(date=(ini_date, fin_date)))
                
                dataDF['total_pop'].iloc[line_counter] = exp_pop.tot_value
                dataDF['exppop_{}'.format(pro_std)].iloc[line_counter] = exp_pop.at_event
                
                gdpa = Exposures()
                gdpa.read_hdf5(gdp_path + 'asset_{}_{}.h5'.format(cnt_iso, str(year)))
                
                imp = Impact()
                imp.calc(gdpa, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['total_asset'].iloc[line_counter] = imp.tot_value
                dataDF['D_CliExp_{}'.format(pro_std)].iloc[line_counter] = imp.at_event

                imp1980 = Impact()
                imp1980.calc(gdpa1980, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['D_1980_{}'.format(pro_std)].iloc[line_counter] = imp1980.at_event
                
                imp2010 = Impact()
                imp2010.calc(gdpa2010, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['D_2010_{}'.format(pro_std)].iloc[line_counter] = imp2010.at_event
                
                gdpa['if_RF'] = 7
                imp = Impact()
                imp.calc(gdpa, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['total_asset'].iloc[line_counter] = imp.tot_value
                dataDF['D_ExpAssCliExp_{}'.format(pro_std)].iloc[line_counter] = imp.at_event

                gdpa1980['if_RF'] = 7
                imp1980 = Impact()
                imp1980.calc(gdpa1980, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['D_ExpAss1980_{}'.format(pro_std)].iloc[line_counter] = imp1980.at_event
                gdpa1980['if_RF'] = conts
                
                gdpa2010['if_RF'] = 7
                imp2010 = Impact()
                imp2010.calc(gdpa2010, if_set, rf.select(date=(ini_date, fin_date)))
                dataDF['D_ExpAss2010_{}'.format(pro_std)].iloc[line_counter] = imp2010.at_event
                gdpa2010['if_RF'] = conts
                
                gdpa1980 = gdpa1980.drop(columns='centr_RF')
                gdpa2010 = gdpa2010.drop(columns='centr_RF')
                
                line_counter+=1
            dataDF.to_csv('/p/projects/ebm/inga/vulnerability/damage_sim_3/results/basin-country-3-damage_{}_{}_{}_{}.csv'.format(args.RF_model, args.CL_model, str(args.cnt0), str(args.cnt1)))
   
    # save output dataframe
    dataDF.to_csv('/p/projects/ebm/inga/vulnerability/damage_sim_3/results/basin-country-3-damage_{}_{}_{}_{}.csv'.format(args.RF_model, args.CL_model, str(args.cnt0), str(args.cnt1)))


