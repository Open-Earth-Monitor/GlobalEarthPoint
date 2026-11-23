import duckdb
import yaml
import geopandas as gpd
import numpy as np
from minio import Minio
import pandas as pd
import requests
import os
import duckdb
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

s3_ip='192.168.49.33:8333'
server_name='shiva'
icesat_product='atl08v7.icesat_20181014_20260720_go_epsg.4326_v20261104'
shpfile='/mnt/slurm/jobs/edtm_modeling/5degree_tile_full.gpkg'
gdf_all = gpd.read_file(shpfile).to_crs('EPSG:4326')
ids=gdf_all.id
# --- Configuration ---
with open("./config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)
    
access_key = config['ACCESS_KEY']
secret_key = config['SECRET_KEY']

s3_config = {
'access_key': access_key,
'secret_access_key': secret_key,
'host': s3_ip,
'bucket': 'global'}
client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 

# 1. Collect all arguments from the directory tree
base_dir = f'/mnt/gaia/raw/IceSAT/tmp-icesat-ard/ATL08v007/{icesat_product}'
args = []

for lon_entry in tqdm(os.scandir(base_dir)):
    if not lon_entry.is_dir():
        continue
    lon = lon_entry.name.split('=')[-1]
    
    for lat_entry in os.scandir(lon_entry.path):
        if not lat_entry.is_dir():
            continue
        lat = lat_entry.name.split('=')[-1]
        
        for year_entry in os.scandir(lat_entry.path):
            if not year_entry.is_dir():
                continue
            year = year_entry.name.split('=')[-1]
            
            block_parquet_paths = [
                i.path for i in os.scandir(year_entry.path) 
                if i.is_file() and i.name.endswith('.parquet')
            ]
            
            if block_parquet_paths:
                args.append((lon, lat, year, block_parquet_paths))

print(f"Total tasks to process: {len(args)}")

# 2. Define the parallel worker function
def worker(lon, lat, year, block_parquet_paths):
    root_partition = f'/mnt/{server_name}/icesat-ard/atl08v007/{icesat_product}/lon={lon}/lat={lat}/year={year}'
    os.makedirs(root_partition, exist_ok=True)
    out_file = f'{root_partition}/lon_{lon}_lat_{lat}_year_{year}_icesat-2_atl08.parquet'
    s3_path = 'glidar/' + '/'.join(out_file.split('/')[3:])
    s3_url = f"http://192.168.1.30:8333/global/{s3_path}"
    # Initialize isolated DuckDB and MinIO instances inside each worker
    con = duckdb.connect()
    con.execute("SET threads = 1;")
    s3_url = f"http://192.168.1.30:8333/global/{s3_path}"
    response = requests.head(s3_url)
    if response.status_code == 200:
        print(f'Tile lon_{lon}_lat_{lat}_year_{year}: already on S3, skipping')
        return
    # Check if the file already exists on S3 via HTTP HEAD
    try:
        response = requests.head(s3_url, timeout=10)
        if response.status_code == 200:
            print(f'Tile lon_{lon}_lat_{lat}_year_{year}: already on S3, skipping')
            return
    except requests.RequestException as e:
        print(f'Warning: HTTP check failed for {s3_url}: {e}')    
    gdf_icesat = con.sql(f"""
        SELECT * 
        FROM read_parquet({block_parquet_paths}, union_by_name=true)
    """).df()

    if len(gdf_icesat) == 0:
        print(f'Tile lon_{lon}_lat_{lat}_year_{year}: no point at all')
        return
        
    gdf_icesat.rename(columns = {'lon_20m':'longitude_20m',
                                     'lat_20m':'latitude_20m'}, inplace = True)
    gdf_icesat = gpd.GeoDataFrame(
        gdf_icesat,
        geometry=gpd.points_from_xy(gdf_icesat.longitude_20m, gdf_icesat.latitude_20m),
        crs="EPSG:4326"
    )


    if len(gdf_icesat) > 1000:
        print(f'Tile lon_{lon}_lat_{lat}_year_{year}: Enough points (n>1000) for visualization')
        gdf_icesat.sample(1000).to_file(
            f'/mnt/{server_name}/icesat-ard/lon.{lon}_lat.{lat}_year.{year}.geojson',
            driver='GeoJSON'
        )

    gdf_icesat.to_parquet(out_file)

    # Initialize MinIO client inside worker to avoid pickling/connection issues
    client = Minio(
        s3_config['host'],
        access_key=s3_config['access_key'],
        secret_key=s3_config['secret_access_key'],
        secure=False
    )
    client.fput_object(s3_config['bucket'], s3_path, out_file)
    print(f'http://192.168.1.30:8333/global/{s3_path} is on S3')
    os.remove(out_file)


# 3. Execute in parallel (unpacking each tuple with *arg)
Parallel(n_jobs=-5)(delayed(worker)(*arg) for arg in args)