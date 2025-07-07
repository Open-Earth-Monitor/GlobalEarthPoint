import geopandas as gpd
from s3fs import S3FileSystem
import numpy as np
from pyarrow.dataset import dataset
import polars as pl
from minio import Minio
import pandas as pd
import requests
import pyarrow.parquet as pq
import os

def generate_tiles(xmin,ymin,xmax,ymax):
    tiles = []
    for lon in range(int(xmin), int(xmax)):
        for lat in range(int(ymin), int(ymax)):
            if lon < 0:
                x = f'{str(-lon).zfill(3)}W'
            else:
                x = f'{str(lon).zfill(3)}E'
            if lat < 0:
                y = f'{str(-lat).zfill(2)}S'
            else:
                y = f'{str(lat).zfill(2)}N'
            tiles.append((f'{x}_{y}'))
    return tiles

access_key=''
access_key_secret=''
s3_ip='192.168.49.33:8333'
server_name='landmark'
olm_icesat_path='atl08v6.icesat_20181014_20230621_go_epsg.4326_v20250315'
tmp_icesat_path='atl08.v006_20181014_20230621_ga_epsg.4326_v20231130.parquet'

shpfile='/mnt/slurm/jobs/edtm_modeling/5degree_tile_full.gpkg'
gdf_all = gpd.read_file(shpfile).to_crs('EPSG:4326')
ids=gdf_all.id

httpfs = S3FileSystem(
    key=access_key,
    secret=access_key_secret,
    endpoint_url=f'http://{s3_ip}'
)

for year in [2018,2019,2020,2021,2022,2023]:
    for idx in ids:
        gdf=gdf_all[gdf_all['id']==idx]
        bbox=' '.join([str(i) for i in np.array(gdf.geometry.buffer(0.00125).bounds)[0]])

        #year=2018
        print(f"Tile {idx} - Start processing")
        tile_df = gdf.bounds
        tile_df=tile_df.apply(lambda x: round(x)).astype(int)
        xmin,ymin,xmax,ymax =np.array(tile_df)[0]

        s3_config = {
        'access_key': access_key,
        'secret_access_key': access_key_secret,
        'host': s3_ip,
        'bucket': 'global'}
        client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 

        root_partition = f'/mnt/{server_name}/icesat-ard/atl08v006/{tmp_icesat_path}/lon={round(xmin)}/lat={round(ymin)}/year={year}'
        os.makedirs(root_partition,exist_ok=True)
        out_file=f'{root_partition}/lon_{round(xmin)}_lat_{round(ymin)}_year_{year}_icesat-2_atl08.parquet'
        s3_path = 'glidar/'+ '/'.join(out_file.split('/')[3:])

        url=f'http://192.168.49.30:8333/global/{s3_path}'
        r = requests.head(url)
        if r.status_code == 200:
            print(f'{url} has been process')
            continue

        tiles=generate_tiles(xmin+0.5,ymin-0.5,xmax+1,ymax+1.1)
        print(f'Tile {idx} tiles numbers:',len(tiles))
        object_path = f'tmp-icesat-ard/ATL08v006/{tmp_icesat_path}'
        dfs=[]
        for tile in tiles:
            try:
                if '000W' in tile:
                    tile_ice = tile.replace('000W','000E')
                    subset_path = object_path + f"/tile={tile_ice}/year={year}"

                elif '00S' in tile:
                    tile_ice = tile.replace('00S','00N')
                    subset_path = object_path + f"/tile={tile_ice}/year={year}"


                else:
                    subset_path = object_path + f"/tile={tile}/year={year}"
                pyarrow_dataset = dataset(source = subset_path ,format = 'parquet',filesystem=httpfs)
                df_default = pl.scan_pyarrow_dataset(pyarrow_dataset).filter(
                                    pl.col('lon_20m')>=round(xmin)).filter(
                                    pl.col('lon_20m')<=round(xmax)).filter(
                                    pl.col('lat_20m')>=round(ymin)).filter(
                                    pl.col('lat_20m')<=round(ymax))

                dfs.append(df_default.collect().to_pandas())
            except FileNotFoundError:
                print(f'{tile} does not exist')
                pass

        if len(dfs)>0:

            gdf_icesat = pd.concat(dfs)

            gdf_icesat = gpd.GeoDataFrame(
                gdf_icesat, geometry=gpd.points_from_xy(gdf_icesat.lon_20m, gdf_icesat.lat_20m), crs="EPSG:4326"
            )
            gdf_icesat.rename(columns = {'lon_20m':'longitude_20m',
                                             'lat_20m':'latitude_20m'}, inplace = True)
        else:
            gdf_icesat=pd.DataFrame([])
        if len(gdf_icesat) < 1000:
            print(f'Tile {idx} does not have sufficient points, skipped')
            continue
        gdf_icesat.sample(1000).to_file(f'/mnt/{server_name}/icesat-ard/lon.{round(xmin)}_lat.{round(ymin)}_year.{year}.geojson',Driver='GeoJSON')
        gdf_icesat.to_parquet(out_file)

        client.fput_object(s3_config['bucket'], s3_path, out_file)
        print(f'http://192.168.1.30:8333/global/{s3_path} is on S3')
        os.remove(out_file)