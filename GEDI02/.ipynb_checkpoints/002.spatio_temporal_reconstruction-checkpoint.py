import os
os.environ['USE_PYGEOS'] = '0'
import h5py
import numpy as np
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm
import geopandas as gpd
from pathlib import Path
from datetime import datetime,timedelta
from eumap import parallel
from eumap.misc import ttprint
import pyarrow as pa
import pyarrow.parquet as pq
import math
import requests
from joblib import Parallel, delayed
import sys
from minio import Minio

start_tile = int(sys.argv[1])
end_tile = int(sys.argv[2])
server_name = sys.argv[3]
dataset_name = 'l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614'
dataset_path = f'/mnt/{server_name}/tmp-gedi-ard/level2/{dataset_name}'


access_key=''
secret_access_key=''
s3_ip='192.168.49.30:8333'
def tile_index_1d(arr):
    x = int(float(arr[0]))
    y = int(float(arr[1]))
    if x < 0:
        ew = 'W'
        x = -x
    else:
        ew = 'E'

    if y < 0:
        ns = 'S'
        y = -y
    else:
        ns = 'N'
    return f'{str(x).zfill(3)}{ew}_{str(y).zfill(2)}{ns}'

landmask=gpd.read_file('gedi_cover_glad_tile_masked.geojson')
def worker(L2A_path, L2B_path):
    try:
        base_template = L2A_path.split('/')[-1][:-3].replace('_A','') + '{i}.parquet'
        year,month = L2A_path.split('/')[-2].split('.')[0:2]
        gediL2A = h5py.File(L2A_path, 'r')  # Read file using h5py
        gediL2A_objs = []
        gediL2A.visit(gediL2A_objs.append)                                           # Retrieve list of datasets
        gediSDS = [o for o in gediL2A_objs if isinstance(gediL2A[o], h5py.Dataset)]  # Search for relevant SDS inside data file
        beamNames = [g for g in gediL2A.keys() if g.startswith('BEAM')]
        delta_time,shotnumber,beamname,latitude,longitude,elev_lowestmode,rh100,rh99,rh98,rh97,rh95,rh75,rh50,rh25,sensitivity,solar_elevation,rh100_a1,rh100_a2,rh100_a3,rh100_a4,rh100_a5,rh100_a6,rh99_a1,rh99_a2,rh99_a3,rh99_a4,rh99_a5,rh99_a6,rh98_a1,rh98_a2,rh98_a3,rh98_a4,rh98_a5,rh98_a6,rh97_a1,rh97_a2,rh97_a3,rh97_a4,rh97_a5,rh97_a6,rh95_a1,rh95_a2,rh95_a3,rh95_a4,rh95_a5,rh95_a6,rh75_a1,rh75_a2,rh75_a3,rh75_a4,rh75_a5,rh75_a6,rh50_a1,rh50_a2,rh50_a3,rh50_a4,rh50_a5,rh50_a6,rh25_a1,rh25_a2,rh25_a3,rh25_a4,rh25_a5,rh25_a6,sensitivity_a1,sensitivity_a2,sensitivity_a3,sensitivity_a4,sensitivity_a5,sensitivity_a6,elev_lowestmode_a1,elev_lowestmode_a2,elev_lowestmode_a3,elev_lowestmode_a4,elev_lowestmode_a5,elev_lowestmode_a6,quality_flag,degrade_flag = ([] for i in range(78))
        for i,b in enumerate(beamNames):       
            # Loop through each beam and open the SDS needed
            [delta_time.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/delta_time') and b in g][0]][()]]    
            [shotnumber.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/shot_number') and b in g][0]][()]]    
            [latitude.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/lat_lowestmode') and b in g][0]][()]]
            [longitude.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/lon_lowestmode') and b in g][0]][()]]
            [elev_lowestmode.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode') and b in g][0]][()]]    
            [rh100.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh99.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh98.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh97.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh95.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh75.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh50.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [rh25.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh') and b in g][0]][()]]
            [sensitivity.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity') and b in g][0]][()]]
            [solar_elevation.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/solar_elevation') and b in g][0]][()]] # solar elevation < 0 indicates night time
            [quality_flag.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/quality_flag') and b in g][0]][()]]
            [degrade_flag.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/degrade_flag') and b in g][0]][()]]

            [rh25_a1.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh25_a2.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh25_a3.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh25_a4.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh25_a5.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh25_a6.append(h[24]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh50_a1.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh50_a2.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh50_a3.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh50_a4.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh50_a5.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh50_a6.append(h[49]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh75_a1.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh75_a2.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh75_a3.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh75_a4.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh75_a5.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh75_a6.append(h[74]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh95_a1.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh95_a2.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh95_a3.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh95_a4.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh95_a5.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh95_a6.append(h[-6]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh97_a1.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh97_a2.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh97_a3.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh97_a4.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh97_a5.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh97_a6.append(h[-4]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh98_a1.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh98_a2.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh98_a3.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh98_a4.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh98_a5.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh98_a6.append(h[-3]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh99_a1.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh99_a2.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh99_a3.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh99_a4.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh99_a5.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh99_a6.append(h[-2]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [rh100_a1.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a1') and b in g][0]][()]]
            [rh100_a2.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a2') and b in g][0]][()]]
            [rh100_a3.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a3') and b in g][0]][()]]
            [rh100_a4.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a4') and b in g][0]][()]]
            [rh100_a5.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a5') and b in g][0]][()]]
            [rh100_a6.append(h[-1]) for h in gediL2A[[g for g in gediSDS if g.endswith('/rh_a6') and b in g][0]][()]]

            [elev_lowestmode_a1.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a1') and b in g][0]][()]]
            [elev_lowestmode_a2.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a2') and b in g][0]][()]]
            [elev_lowestmode_a3.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a3') and b in g][0]][()]]
            [elev_lowestmode_a4.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a4') and b in g][0]][()]]
            [elev_lowestmode_a5.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a5') and b in g][0]][()]]
            [elev_lowestmode_a6.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/elev_lowestmode_a6') and b in g][0]][()]]

            [sensitivity_a1.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a1') and b in g][0]][()]]
            [sensitivity_a2.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a2') and b in g][0]][()]]
            [sensitivity_a3.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a3') and b in g][0]][()]]
            [sensitivity_a4.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a4') and b in g][0]][()]]
            [sensitivity_a5.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a5') and b in g][0]][()]]
            [sensitivity_a6.append(h) for h in gediL2A[[g for g in gediSDS if g.endswith('/sensitivity_a6') and b in g][0]][()]]

            beamname += [i]*len([h for h in gediL2A[[g for g in gediSDS if g.endswith('/delta_time') and b in g][0]][()]])

        arr_l2a = np.array((delta_time,beamname,shotnumber,latitude,longitude,elev_lowestmode,rh100,rh99,rh98,rh97,rh95,rh75,rh50,rh25,sensitivity,solar_elevation,rh100_a1,rh100_a2,rh100_a3,rh100_a4,rh100_a5,rh100_a6,rh99_a1,rh99_a2,rh99_a3,rh99_a4,rh99_a5,rh99_a6,rh98_a1,rh98_a2,rh98_a3,rh98_a4,rh98_a5,rh98_a6,rh97_a1,rh97_a2,rh97_a3,rh97_a4,rh97_a5,rh97_a6,rh95_a1,rh95_a2,rh95_a3,rh95_a4,rh95_a5,rh95_a6,rh75_a1,rh75_a2,rh75_a3,rh75_a4,rh75_a5,rh75_a6,rh50_a1,rh50_a2,rh50_a3,rh50_a4,rh50_a5,rh50_a6,rh25_a1,rh25_a2,rh25_a3,rh25_a4,rh25_a5,rh25_a6,sensitivity_a1,sensitivity_a2,sensitivity_a3,sensitivity_a4,sensitivity_a5,sensitivity_a6,elev_lowestmode_a1,elev_lowestmode_a2,elev_lowestmode_a3,elev_lowestmode_a4,elev_lowestmode_a5,elev_lowestmode_a6))
        arr_l2a_filtered = arr_l2a[:,np.logical_and(np.logical_and(np.array(sensitivity) >=0.95,np.array(quality_flag) != 0),np.array(degrade_flag) <= 0)]
        arr_l2a=None
        df = pd.DataFrame(arr_l2a_filtered[3:5,:].T,columns=['latitude','longitude'])
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
        )
        arr_slice = gpd.sjoin(gdf,landmask).index
        gdf=None
        df=None
        arr_l2a_filtered = arr_l2a_filtered[:,arr_slice]
    except:
        ttprint(f'ERROR in {L2A_path}')
        return
    try:
        if arr_l2a_filtered.shape[1] != 0:
            ttprint(f'{L2A_path} has valid data points')

            l2a_dot = np.array([1,1,1,1,1,100,100,100,100,100,100,100,100,100,10000,1,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,100,10000,10000,10000,10000,10000,10000,100,100,100,100,100,100])
            #print(L2B_path)
            #print(f'/mnt/gaia/raw/GEDI/GEDI02_B.002/{L2B_path.split("/")[-2]}')
            #print(L2B_path.split("/")[-1][:-18])
            L2B_actual_path = [i.path for i in os.scandir(f'/mnt/gaia/raw/GEDI/GEDI02_B.002/{L2B_path.split("/")[-2]}') if L2B_path.split("/")[-1][:-18] in i.name][0]
            #print([i.path for i in os.scandir(f'/mnt/gaia/raw/GEDI/GEDI02_B.002/{L2B_path.split("/")[-2]}') if L2B_path.split("/")[-1][:-18] in i.name])
            #print(L2B_actual_path)
            gediL2B = h5py.File(L2B_actual_path, 'r')  # Read file using h5py
            gediL2B_objs = []
            gediL2B.visit(gediL2B_objs.append)                                           # Retrieve list of datasets
            gediSDS = [o for o in gediL2B_objs if isinstance(gediL2B[o], h5py.Dataset)]  # Search for relevant SDS inside data file

            cover,num_detectedmodes,omega,pai,pgap_theta,rg,rv,rhog,selected_rg_algorithm,rhov,selected_l2a_algorithm,fhd_normal,surface_flag,leaf_off_flag,l2b_quality_flag = ([] for i in range(15))
            for b in beamNames:   
                # Loop through each beam and open the SDS needed
                [cover.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/cover') and b in g][0]][()]]    
                [num_detectedmodes.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/num_detectedmodes') and b in g][0]][()]]
                [omega.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/omega') and b in g][0]][()]]
                [pai.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/pai') and b in g][0]][()]]
                [pgap_theta.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/pgap_theta') and b in g][0]][()]]
                [rg.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/rg') and b in g][0]][()]]
                [rv.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/rv') and b in g][0]][()]]
                [rhog.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/rhog') and b in g][0]][()]]
                [selected_rg_algorithm.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/selected_rg_algorithm') and b in g][0]][()]]    
                [rhov.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/rhov') and b in g][0]][()]]
                [selected_l2a_algorithm.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/selected_l2a_algorithm') and b in g][0]][()]]
                [fhd_normal.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/fhd_normal') and b in g][0]][()]]    
                [surface_flag.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/surface_flag') and b in g][0]][()]]
                [leaf_off_flag.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/leaf_off_flag') and b in g][0]][()]]
                [l2b_quality_flag.append(h) for h in gediL2B[[g for g in gediSDS if g.endswith('/l2b_quality_flag') and b in g][0]][()]]


            arr_l2b = np.array((cover,num_detectedmodes,omega,pai,pgap_theta,rg,rv,rhog,selected_rg_algorithm,rhov,selected_l2a_algorithm,fhd_normal,surface_flag,leaf_off_flag,l2b_quality_flag))

            l2b_dot = np.array([10000,1,10000,1000,10000,10,10,10000,1,10000,1,100,1,1,1])


            arr_l2b_filtered = arr_l2b[:,np.logical_and(np.logical_and(np.array(sensitivity) >=0.95,np.array(quality_flag) != 0),np.array(degrade_flag) <= 0)]
            arr_l2b_filtered = arr_l2b_filtered[:,arr_slice]
            arr_l2 = np.hstack((arr_l2a_filtered.T*l2a_dot, arr_l2b_filtered.T*l2b_dot))
            arr_l2[~np.isfinite(arr_l2)] = -2147483648
            df = pd.DataFrame(
                arr_l2,
                columns = ['delta_time','beamname','shotnumber','latitude','longitude','elev_lowestmode',
                           'rh100','rh99','rh98','rh97','rh95','rh75','rh50','rh25','sensitivity',
                           'solar_elevation','rh100_a1','rh100_a2','rh100_a3','rh100_a4','rh100_a5',
                           'rh100_a6','rh99_a1','rh99_a2','rh99_a3','rh99_a4','rh99_a5','rh99_a6',
                           'rh98_a1','rh98_a2','rh98_a3','rh98_a4','rh98_a5','rh98_a6','rh97_a1',
                           'rh97_a2','rh97_a3','rh97_a4','rh97_a5','rh97_a6','rh95_a1','rh95_a2',
                           'rh95_a3','rh95_a4','rh95_a5','rh95_a6','rh75_a1','rh75_a2','rh75_a3',
                           'rh75_a4','rh75_a5','rh75_a6','rh50_a1','rh50_a2','rh50_a3','rh50_a4',
                           'rh50_a5','rh50_a6','rh25_a1','rh25_a2','rh25_a3','rh25_a4','rh25_a5',
                           'rh25_a6','sensitivity_a1','sensitivity_a2','sensitivity_a3',
                           'sensitivity_a4','sensitivity_a5','sensitivity_a6','elev_lowestmode_a1',
                           'elev_lowestmode_a2','elev_lowestmode_a3','elev_lowestmode_a4',
                           'elev_lowestmode_a5','elev_lowestmode_a6','cover','num_detectedmodes',
                           'omega','pai','pgap_theta','rg','rv','rhog','selected_rg_algorithm',
                           'rhov','selected_l2a_algorithm','fhd_normal','surface_flag',
                           'leaf_off_flag','l2b_quality_flag'])
            arr_l2b=None
            arr_l2=None
            df['night_flag'] = df['solar_elevation']<0
            df['surface_flag'] = df['surface_flag']==1
            df['l2b_quality_flag'] = df['l2b_quality_flag']==1
            df['leaf_off_flag'] = df['leaf_off_flag']==1

            df['year'] = year
            df['lat'] = df['latitude'].apply(lambda x: math.floor(x/5)*5)
            df['lon'] = df['longitude'].apply(lambda x: math.floor(x/5)*5)

            pd_dtype = {'delta_time':np.int64,'beamname':np.uint8,'shotnumber':np.int64,'latitude':np.float64,
                        'longitude':np.float64,'elev_lowestmode':np.int32,'rh100':np.int16,'rh99':np.int16,
                        'rh98':np.int16,'rh97':np.int16,'rh95':np.int16,'rh75':np.int16,'rh50':np.int16,
                        'rh25':np.int16,'sensitivity':np.int16,'night_flag':bool,'rh100_a1':np.int16,
                        'rh100_a2':np.int16,'rh100_a3':np.int16,'rh100_a4':np.int16,'rh100_a5':np.int16,
                        'rh100_a6':np.int16,'rh99_a1':np.int16,'rh99_a2':np.int16,'rh99_a3':np.int16,
                        'rh99_a4':np.int16,'rh99_a5':np.int16,'rh99_a6':np.int16,'rh98_a1':np.int16,
                        'rh98_a2':np.int16,'rh98_a3':np.int16,'rh98_a4':np.int16,'rh98_a5':np.int16,
                        'rh98_a6':np.int16,'rh97_a1':np.int16,'rh97_a2':np.int16,'rh97_a3':np.int16,
                        'rh97_a4':np.int16,'rh97_a5':np.int16,'rh97_a6':np.int16,'rh95_a1':np.int16,
                        'rh95_a2':np.int16,'rh95_a3':np.int16,'rh95_a4':np.int16,'rh95_a5':np.int16,
                        'rh95_a6':np.int16,'rh75_a1':np.int16,'rh75_a2':np.int16,'rh75_a3':np.int16,
                        'rh75_a4':np.int16,'rh75_a5':np.int16,'rh75_a6':np.int16,'rh50_a1':np.int16,
                        'rh50_a2':np.int16,'rh50_a3':np.int16,'rh50_a4':np.int16,'rh50_a5':np.int16,
                        'rh50_a6':np.int16,'rh25_a1':np.int16,'rh25_a2':np.int16,'rh25_a3':np.int16,
                        'rh25_a4':np.int16,'rh25_a5':np.int16,'rh25_a6':np.int16,'sensitivity_a1':np.int16,
                        'sensitivity_a2':np.int16,'sensitivity_a3':np.int16,'sensitivity_a4':np.int16,
                        'sensitivity_a5':np.int16,'sensitivity_a6':np.int16,'elev_lowestmode_a1':np.int16,
                        'elev_lowestmode_a2':np.int16,'elev_lowestmode_a3':np.int16,'elev_lowestmode_a4':np.int16,
                        'elev_lowestmode_a5':np.int16,'elev_lowestmode_a6':np.int16,'cover':np.int16,'num_detectedmodes':np.uint8,
                        'omega':np.int16,'pai':np.int16,'pgap_theta':np.int16,'rg':np.int32,'rv':np.int32,'rhog':np.int16,
                        'selected_rg_algorithm':np.uint8,'rhov':np.int16,'selected_l2a_algorithm':np.uint8,'fhd_normal':np.int16,
                        'surface_flag':bool,'leaf_off_flag':bool,'l2b_quality_flag':bool,'lon':np.int16,'lat':np.int16,'year':np.uint16}


            data = df[['delta_time','beamname','shotnumber','latitude','longitude','elev_lowestmode',
                           'rh100','rh99','rh98','rh97','rh95','rh75','rh50','rh25','sensitivity',
                           'night_flag','rh100_a1','rh100_a2','rh100_a3','rh100_a4','rh100_a5',
                           'rh100_a6','rh99_a1','rh99_a2','rh99_a3','rh99_a4','rh99_a5','rh99_a6',
                           'rh98_a1','rh98_a2','rh98_a3','rh98_a4','rh98_a5','rh98_a6','rh97_a1',
                           'rh97_a2','rh97_a3','rh97_a4','rh97_a5','rh97_a6','rh95_a1','rh95_a2',
                           'rh95_a3','rh95_a4','rh95_a5','rh95_a6','rh75_a1','rh75_a2','rh75_a3',
                           'rh75_a4','rh75_a5','rh75_a6','rh50_a1','rh50_a2','rh50_a3','rh50_a4',
                           'rh50_a5','rh50_a6','rh25_a1','rh25_a2','rh25_a3','rh25_a4','rh25_a5',
                           'rh25_a6','sensitivity_a1','sensitivity_a2','sensitivity_a3',
                           'sensitivity_a4','sensitivity_a5','sensitivity_a6','elev_lowestmode_a1',
                           'elev_lowestmode_a2','elev_lowestmode_a3','elev_lowestmode_a4',
                           'elev_lowestmode_a5','elev_lowestmode_a6','cover','num_detectedmodes',
                           'omega','pai','pgap_theta','rg','rv','rhog','selected_rg_algorithm',
                           'rhov','selected_l2a_algorithm','fhd_normal','surface_flag',
                           'leaf_off_flag','l2b_quality_flag','lon','lat','year']].astype(pd_dtype)
            df=None
            ttprint(f'L2 {base_template} writes to parquet dataset')
            pq.write_to_dataset(pa.Table.from_pandas(data),
                    dataset_path,
                    partition_cols=['lon','lat','year'],
                    existing_data_behavior='overwrite_or_ignore',
                    basename_template=base_template,
                    compression="snappy",
                    version="2.4")
        else:
            ttprint(f'no valid point in {L2B_path}')
            return
    except:
        ttprint(f'ERROR in {L2B_path}')
        return


for result in parallel.job(worker, args, n_jobs=-5):
    print(result)

## push to gaia
files = [] 
for lon_pat in os.scandir(f"{dataset_path}"):
    for lat_pat in os.scandir(lon_pat.path):
        for year_pat in os.scandir(lat_pat.path):
            for file in os.scandir(year_pat.path):
                files.append(file.path)
    
def push_to_s3(file):
    s3_config = {
    'access_key': access_key,
    'secret_access_key': secret_access_key,
    'host': s3_ip,
    'bucket': 'tmp-gedi-ard'}
    client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 
    file_name = file.split('/')[-1]
    year_partition = file.split('/')[-2]
    lat_partition = file.split('/')[-3]
    lon_partition = file.split('/')[-4]
    url=f'http://192.168.49.30:8333/tmp-gedi-ard/level2/{dataset_name}/{lon_partition}/{lat_partition}/{year_partition}/{file_name}'
    r = requests.head(url)
    if r.status_code == 200:
        print(f'{url} has been process')
        os.remove(file)
        return
    s3_path = f"level2/{dataset_name}/{lon_partition}/{lat_partition}/{year_partition}/{file_name}"
    client.fput_object(s3_config['bucket'], s3_path, file)
    os.remove(file)
    ttprint(f'http://192.168.1.30:8333/tmp-gedi-ard/{s3_path} on S3')

Parallel(n_jobs=1)(delayed(push_to_s3)(i) for i in files)
