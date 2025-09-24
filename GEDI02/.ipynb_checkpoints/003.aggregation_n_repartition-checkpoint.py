import pyarrow.parquet as pq
import geopandas as gpd
import numpy as np
from joblib import Parallel, delayed
import os
import sys
from minio import Minio
from s3fs import S3FileSystem

import requests

access_key=''
secret_access_key=''
s3_ip='192.168.49.30:8333'
olm_gedi_path='l2v002.gedi_20190418_20230316_go_epsg.4326_v20250707'
tmp_gedi_path='l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614'
s3_config = {
'access_key': access_key,
'secret_access_key': secret_access_key,
'host': s3_ip,
'bucket': 'tmp-gedi-ard'}
client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 

server_name = sys.argv[1]

args=[]
objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{tmp_gedi_path}/")
for obj in objects:
    lon_i = obj.object_name.split('=')[-1].split('/')[0]
    sub_objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{tmp_gedi_path}/lon={lon_i}/")
    for sub_obj in sub_objects:
        lat_i = sub_obj.object_name.split('=')[-1].split('/')[0]
        sub_sub_objects = client.list_objects("tmp-gedi-ard", prefix=f"level2/{tmp_gedi_path}/lon={lon_i}/lat={lat_i}/")
        for sub_sub_obj in sub_sub_objects:
            year = sub_sub_obj.object_name.split('=')[-1].split('/')[0]
            args.append([int(lon_i) ,int(lat_i),int(year)])
            
print(args[100])
print(f'total number of parititon: {len(args)}')

os.makedirs(f'/mnt/{server_name}/tmp-gedi-ard/level2/{olm_gedi_path}',exist_ok=True)
            
def worker(info):
#for info in args:

    httpfs = S3FileSystem(
          key=access_key,
          secret=secret_access_key,
          endpoint_url=f'http://{s3_ip}'
       )
    
    s3_config = {
    'access_key': access_key,
    'secret_access_key': secret_access_key,
    'host': s3_ip,
    'bucket': 'gedi-ard'}
    client = Minio(s3_config['host'], s3_config['access_key'], s3_config['secret_access_key'], secure=False) 

    p = info[0]
    j = info[1]
    k = info[2]
    url =f'https://{ip}/global/glidar/gedi-ard/level2/{olm_gedi_path}/lon={p}/lat={j}/year={k}/gedi_l2_lon_{p}_lat={j}_year_{k}_0.parquet'
    r = requests.head(url)
    if r.status_code == 200:
        print(f'{url} is processed')
        return
    tmp = pq.ParquetDataset(path_or_paths=f'tmp-gedi-ard/level2/{tmp_gedi_path}',
                            filesystem=httpfs,
                            filters=[('lon','=',p),('lat','=',j),('year','=',k)])
    data = tmp.read()
    pq.write_to_dataset(data,
            f"/mnt/{server_name}/tmp-gedi-ard/level2/{olm_gedi_path}",
            partition_cols=['lon','lat','year'],
            existing_data_behavior='delete_matching',
            basename_template=f'gedi_l2_lon_{p}_lat={j}_year_{k}_'+ '{i}.parquet',
            compression="snappy",
            version="2.4")
    out_file = f'/mnt/{server_name}/tmp-gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240827/lon={i}/lat={j}/year={k}/gedi_l2_lon_{p}_lat={j}_year_{k}_0.parquet'
    s3_path = f'level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240622/lon={i}/lat={j}/year={k}/gedi_l2_lon_{p}_lat={j}_year_{k}_0.parquet'
    
    client.fput_object(s3_config['bucket'], s3_path, out_file)
    print(f'https://{s3_ip}/global/glidar/gedi-ard/level2/{olm_gedi_path}/lon={i}/lat={j}/year={k}/gedi_l2_lon_{p}_lat={j}_year_{k}_0.parquet is on S3')
    os.remove(out_file)
    
Parallel(n_jobs=30)(delayed(worker)(i) for i in args)
