import pyarrow.parquet as pq
import geopandas as gpd
import numpy as np
from joblib import Parallel, delayed
import os
import sys
from minio import Minio
from s3fs import S3FileSystem
from tqdm import tqdm
import requests


access_key=''
secret_access_key=''
s3_ip='192.168.49.30:8333'

s3_config = {
'access_key': access_key,
'secret_access_key': secret_access_key,
'host': s3_ip,
'bucket': 'tmp-gedi-ard'}
client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 

version='l2v002.gedi_20190418_20230316_go_epsg.4326_v20250820'
server_name='apollo'


args=[]
objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{version}/")
for obj in tqdm(objects):
    lon_i = obj.object_name.split('=')[-1].split('/')[0]
    sub_objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{version}/lon={lon_i}/")
    for sub_obj in sub_objects:
        lat_i = sub_obj.object_name.split('=')[-1].split('/')[0]
        sub_sub_objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{version}/lon={lon_i}/lat={lat_i}/")
        for sub_sub_obj in sub_sub_objects:
            year = sub_sub_obj.object_name.split('=')[-1].split('/')[0]
            args.append([int(lon_i) ,int(lat_i),int(year)])

def worker(info):
    httpfs = S3FileSystem(
          key=access_key,
          secret=secret_access_key,
          endpoint_url=f'http://{s3_ip}'
       )

    s3_config = {
    'access_key': access_key,
    'secret_access_key': secret_access_key,
    'host': s3_ip,
    'bucket': 'global'}

    client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 
    i = info[0]
    j = info[1]
    k = info[2]
    out_folder=f'/mnt/{server_name}/gedi-ard/level2/{version}/lon={i}/lat={j}/year={k}'
    out_file = f'{out_folder}/gedi_l2ab_lon={i}_lat={j}_year={k}.parquet'
    s3_path = 'glidar/'+ '/'.join(out_file.split('/')[3:])
    url =f'http://{s3_ip}/global/{s3_path}'
    r = requests.head(url)
    if r.status_code == 200:
        #print(f'{url} is processed')
        pass
    else:
        try:
            tmp = pq.ParquetDataset(path_or_paths=f'tmp-gedi-ard/level2/{version}',
                                    filesystem=httpfs,
                                    filters=[('lon','=',i),('lat','=',j),('year','=',k)])
            df = tmp.read().to_pandas()
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']),crs='EPSG:4326')

            os.makedirs(out_folder,exist_ok=True)

            gdf.to_parquet(out_file)


            client.fput_object(s3_config['bucket'], s3_path, out_file)
            os.remove(out_file)
        except Exception as e:
            print(e)
            
Parallel(n_jobs=30)(delayed(worker)(i) for i in args)