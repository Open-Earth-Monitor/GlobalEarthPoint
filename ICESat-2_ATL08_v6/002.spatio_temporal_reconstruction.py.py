import h5py
import numpy as np
import pandas as pd
import geopandas as gpd
import os
os.environ['WR_S3_ENDPOINT_URL'] = 'http://192.168.49.30:8333'
from datetime import datetime
from eumap import parallel
from eumap.misc import ttprint
from minio import Minio
import requests
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
import awswrangler as wr
import sys
import boto3

access_key=""
aws_secret_access_key=""
out_path="atl08.v006_20181014_20230621_ga_epsg.4326_v20231130.parquet"
endpoint = "192.168.49.30:8333"


sess = boto3.Session(aws_access_key_id=access_key,
                     aws_secret_access_key=aws_secret_access_key)

s3_path = f"tmp-icesat-ard/ATL08v006/{out_path}"
scheme = 'http'
bucket = 'tmp-icesat-ard'
httpfs = pa.fs.S3FileSystem(scheme=scheme,
    access_key = access_key,
    secret_key = aws_secret_access_key,
    endpoint_override=endpoint)

s3_options = {
    "key": access_key,
   "secret": aws_secret_access_key,
    "client_kwargs": {"endpoint_url": 'http://'+endpoint}
}

s3=s3fs.S3FileSystem(**s3_options)

def binary_search(arr, target):
    left, right = 0, len(arr) - 1
    while left <= right:
        mid = left + (right - left) // 2
        if arr[mid] == target:
            inds = []
            while mid > 0 and arr[mid - 1] == target:
                mid -= 1
            while arr[mid] == target:
                inds.append(mid)
                mid+=1
                if mid+1 > len(arr):
                    break
            return inds
        elif arr[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    return None

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


def tile_index_10d(arr):
    x = int(np.floor(float(arr[0])*0.1)*10)
    y = int(np.floor(float(arr[1])*0.1)*10)
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

#def _processed(host, output_file, bucket_name, out_pref):
#
#  url = f'http://{host}/{bucket_name}/{out_pref}/{output_file}'
#  r = requests.head(url)
#  return (r.status_code == 200)

def worker(fname,g):

    def stat(signal_info):
        ph_str = signal_info[0]
        flag_str = signal_info[1]
        d_flag_str = signal_info[2]
        ph_hs_str = np.array([i for i in ph_str.split(',')])
        d_flag = np.array([i for i in d_flag_str.split(',')])
        classed_flag = np.array([i for i in flag_str.split(',')])
        
        te = classed_flag == '1'
        ca = classed_flag == '2'
        tca = classed_flag == '3'
        ph_h_canopy = ','.join(ph_hs_str[ca])
        ph_h_tcanopy = ','.join(ph_hs_str[tca])
        ph_hs = np.array([np.float32(ph_h_str) for ph_h_str in ph_hs_str])
        n_ph_te_20m = len(ph_hs[te])
        n_ph_20m = len(ph_hs)
        h_te_std_20m = np.std(ph_hs[te])
        ph_hs_can = np.append(ph_hs[ca],ph_hs[tca])
        d_flag_ca = ','.join(d_flag[ca])
        d_flag_tca  = ','.join(d_flag[tca])
        med_ht = np.quantile(ph_hs_can, 0.5) if ph_hs_can.size > 0 else None
        iqr_ht = (np.quantile(ph_hs_can, 0.75) - np.quantile(ph_hs_can, 0.25)) if ph_hs_can.size > 0 else None
        p90_ht = np.quantile(ph_hs_can, 0.9) if ph_hs_can.size > 0 else None 
        p95_ht = np.quantile(ph_hs_can, 0.95) if ph_hs_can.size > 0 else None
        
        def bin_hist(x):
            bins = [0, 1, 3, 5, 15, 50]
            hist, _ = np.histogram(x, bins=bins)
            return hist

        bins = bin_hist(ph_hs_can)
        bin0, bin1, bin2, bin3, bin4 = [binx for binx in bins]
        return n_ph_20m,n_ph_te_20m,h_te_std_20m,med_ht,iqr_ht,p90_ht,p95_ht,bin0, bin1, bin2, bin3, bin4, ph_h_canopy, ph_h_tcanopy, d_flag_ca, d_flag_tca
    
    def ph_extract(seg_id_range):
        ph_20m_seg_ids = [binary_search(ph_segment_id,seg_number) for seg_number in np.arange(seg_id_range[0],seg_id_range[1])]
        pc_flag_20m_seg = [classed_pc_flag[seg_id] for seg_id in ph_20m_seg_ids]
        ph_h_20m_seg = [','.join(map(str,ph_h[seg_id])) for seg_id in ph_20m_seg_ids]
        d_flag_20m_seg = [','.join(map(str,d_flag[seg_id])) for seg_id in ph_20m_seg_ids]
        valid_flag = [bool(np.where(len(signal_flags)>=10 and (sum(signal_flags==1)>=3 or (sum(signal_flags==1)>=3 and sum(signal_flags>1)>=3)),True,False)) for signal_flags in pc_flag_20m_seg]
        ph_h_seg_20m_valid = np.array(ph_h_20m_seg)[valid_flag]
        pc_flag_inds = [','.join(map(str,pc_flag_ind)) for pc_flag_ind in pc_flag_20m_seg]
        pc_flag_20m_seg_valid = np.array(pc_flag_inds)[valid_flag]
        d_flag_20m_seg_valid = np.array(d_flag_20m_seg)[valid_flag]
        return np.array([ph_h_seg_20m_valid, pc_flag_20m_seg_valid, d_flag_20m_seg_valid])
    try:
        output_file = g + '_' + fname.split('/')[-1][:-3]
        base_template = g[1:] + '_' + fname.split('/')[-1][:-3]
        year = fname.split('/')[-2][:4]
        month = fname.split('/')[-2][5:7]
        #ttprint(f'processing {output_file}')
        with h5py.File(fname, 'r') as fi:
            lat_100m = fi[g+'/land_segments/latitude'][:]
            lon_100m = fi[g+'/land_segments/longitude'][:]
            anch_tile = tile_index_1d([lon_100m[-1],lat_100m[-1]])
            object_path = f'{s3_path}/tile={anch_tile}/year={int(year)}/month={int(month)}/'
            #print(s3.ls(object_path))
            if s3.isdir(object_path):                
                if len([i for i in s3.ls(object_path) if base_template in i])>0:
                    ttprint(f'{base_template} is computed')
                    return

            #ttprint(f"start reading segment {g}_{fname.split('/')[-1]}")
            start_dt = fi['/ancillary_data/data_start_utc'][:]
            end_dt = fi['/ancillary_data/data_end_utc'][:]
            sc_orient = fi['orbit_info']['sc_orient'][:]

            canopy20m_thresh = fi['/ancillary_data/land/canopy20m_thresh'][:]
            stat20m_thresh = fi['/ancillary_data/land/stat20m_thresh'][:]
            terrain20m_thresh = fi['/ancillary_data/land/terrain20m_thresh'][:]


            start_dt = fi['/ancillary_data/data_start_utc'][:]
            end_dt = fi['/ancillary_data/data_end_utc'][:]

            lat = fi[g+'/land_segments/latitude_20m'][:]
            lon = fi[g+'/land_segments/longitude_20m'][:]
            #canopy_h_metrics = fi[g+'/land_segments/canopy/canopy_h_metrics'][:]
            h_canopy_20m = fi[g+'/land_segments/canopy/h_canopy_20m'][:]
            h_canopy = fi[g+'/land_segments/canopy/h_canopy'][:]

            layer_flag = fi[g+'/land_segments/layer_flag'][:]
            night_flag = fi[g+'/land_segments/night_flag'][:]
            cloud_flag_atm = fi[g+'/land_segments/cloud_flag_atm'][:]

            ## flag
            segment_id_beg = fi[g+'/land_segments/segment_id_beg'][:]
            segment_id_end = fi[g+'/land_segments/segment_id_end'][:]
            classed_pc_flag = fi[g+'/signal_photons/classed_pc_flag'][:]
            classed_pc_indx = fi[g+'/signal_photons/classed_pc_indx'][:]
            d_flag = fi[g+'/signal_photons/d_flag'][:]

            ph_h = fi[g+'/signal_photons/ph_h'][:]
            ph_segment_id = fi[g+'/signal_photons/ph_segment_id'][:]


            dem_flag = fi[g+'/land_segments/dem_flag'][:]
            sat_flag = fi[g+'/land_segments/sat_flag'][:]
            dem_removal_flag = fi[g+'/land_segments/dem_removal_flag'][:]
            h_dif_ref = fi[g+'/land_segments/h_dif_ref'][:]
            terrain_flg = fi[g+'/land_segments/terrain_flg'][:]

            sc_orient = fi['orbit_info']['sc_orient'][:]

            h_te_best_fit_20m = fi[g+'/land_segments/terrain/h_te_best_fit_20m'][:]
            h_te_best_fit = fi[g+'/land_segments/terrain/h_te_best_fit'][:]
            #ttprint(f"end reading segment {g}_{fname.split('/')[-1]}")


        ttprint(f"start 20m segment processing {g}_{fname.split('/')[-1]}")
        seg_idx = np.vstack((segment_id_beg,segment_id_end+1)).transpose()
        signal_photons = list(map(lambda x: ph_extract(x),seg_idx))
        flags_20m = np.logical_or(h_te_best_fit_20m<1e+38,h_canopy_20m<1e+38)

        info_str = np.empty((4,flags_20m.shape[0]*5), dtype="object")
        info_20m_seg = np.full((16,flags_20m.shape[0]*5),-9999.0)
        info_100m_seg = np.empty((10,flags_20m.shape[0]*5))
        start_ind = 0

        for seg in range(flags_20m.shape[0]):
            flag_20m = flags_20m[seg,:]
            h_te_best_fit_20m_valid = h_te_best_fit_20m[seg][flag_20m]
            h_canopy_20m_valid = h_canopy_20m[seg][flag_20m]
            lat_20m_valid = lat[seg][flag_20m]
            lon_20m_valid = lon[seg][flag_20m]
            num_20mseg = len(h_canopy_20m_valid)
            end_ind = start_ind + num_20mseg
            if num_20mseg == len(signal_photons[seg].T) and num_20mseg>0:
                stats = list(map(lambda x: stat(x),signal_photons[seg].T.tolist()))
                n_ph_20m = [i[0] for i in stats]
                n_ph_te_20m = [i[1] for i in stats]
                h_te_std_20m = [i[2] for i in stats]
                med_ht = [i[3] for i in stats]
                iqr_ht = [i[4] for i in stats]
                p90_ht = [i[5] for i in stats]
                p95_ht = [i[6] for i in stats]
                bin0 = [i[7] for i in stats]
                bin1 = [i[8] for i in stats]
                bin2 = [i[9] for i in stats]
                bin3 = [i[10] for i in stats]
                bin4 = [i[11] for i in stats]
                ph_hs_ca = [i[12] for i in stats]
                ph_hs_tca = [i[13] for i in stats]
                d_flag_ca = [i[14] for i in stats]
                d_flag_tca = [i[15] for i in stats]
                info_20m_seg[:,start_ind:end_ind] = np.vstack((np.array(lat_20m_valid),np.array(lon_20m_valid),
                                                               np.array(h_te_best_fit_20m_valid),np.array(h_canopy_20m_valid),
                                                               np.array(n_ph_20m),np.array(n_ph_te_20m),np.array(h_te_std_20m),
                                                               np.array(med_ht),np.array(iqr_ht),np.array(p90_ht),np.array(p95_ht),
                                                               np.array(bin0),np.array(bin1),np.array(bin2),np.array(bin3),np.array(bin4)))
                info_str[:,start_ind:end_ind] =  np.vstack((np.array(ph_hs_ca),np.array(ph_hs_tca),np.array(d_flag_ca),np.array(d_flag_tca)))
                info_100m_seg[:,start_ind:end_ind] = np.vstack((np.repeat(layer_flag[seg],num_20mseg),
                                                                np.repeat(night_flag[seg],num_20mseg),
                                                                np.repeat(cloud_flag_atm[seg],num_20mseg),
                                                                np.repeat(sat_flag[seg],num_20mseg),
                                                                np.repeat(dem_flag[seg],num_20mseg),
                                                                np.repeat(dem_removal_flag[seg],num_20mseg),
                                                                np.repeat(h_dif_ref[seg],num_20mseg),
                                                                np.repeat(terrain_flg[seg],num_20mseg),
                                                                np.repeat(h_te_best_fit[seg],num_20mseg),
                                                                np.repeat(h_canopy[seg],num_20mseg)))
            start_ind += num_20mseg
        ttprint(f"end 20m segment processing {g}_{fname.split('/')[-1]}")
        ttprint(f"start make dataframe {g}_{fname.split('/')[-1]}")
        valid_seg = info_20m_seg[0,:]!=-9999.0
        act_info_20m_seg = info_20m_seg[:,valid_seg]
        act_info_20m_seg[act_info_20m_seg>1e+38] = np.nan
        act_info_100m_seg = info_100m_seg[:,valid_seg]
        act_info_100m_seg[act_info_100m_seg>1e+38] = np.nan
        act_info_str = info_str[:,valid_seg]
        df = pd.DataFrame(
            np.concatenate((act_info_20m_seg,act_info_100m_seg), axis=0).T,
            columns = ['lat_20m','lon_20m','h_te_best_fit_20m','h_canopy_20m','n_ph_20m',
                       'n_ph_te_20m','h_te_std_20m','med_ht','iqr_ht','p90_ht','p95_ht','bin0','bin1','bin2','bin3','bin4',
                       'layer_flag','night_flag','cloud_flag_atm','sat_flag','dem_flag',
                       'dem_removal_flag','h_dif_ref','terrain_flg','h_te_best_fit','h_canopy'])
        df_str = pd.DataFrame(act_info_str.T,columns=['ph_h_canopy','ph_h_tcanopy','d_flag_ca','d_flag_tca'])
        df_final = pd.concat([df_str,df],axis=1)
        if len(df_final) > 0:
            df_final['sc_orient'] = sc_orient[0]
            if sc_orient == 1 and g[-1] == 'r':
                df_final['beam_strength'] = 'strong'
            elif sc_orient == 0 and g[-1] == 'l':
                df_final['beam_strength'] = 'strong'
            else:
                df_final['beam_strength'] = 'weak'
            df_final['start_dt'] = datetime.strptime(start_dt[0].decode('utf-8'),'%Y-%m-%dT%H:%M:%S.%fZ')
            df_final['end_dt'] = datetime.strptime(end_dt[0].decode('utf-8'),'%Y-%m-%dT%H:%M:%S.%fZ')
            df_final['tile'] =  np.apply_along_axis(tile_index_1d, 1, df[['lon_20m','lat_20m']].to_numpy())
            df_final['year'] = year
            df_final['month'] = month
            #ttprint(f"end making dataframe {g}_{fname.split('/')[-1]}")
            #output_fn_file = f'/mnt/apollo/ensemble_DTM_faen/icesatv006{output_file}.pq'
            pd_dtype = {'lat_20m': np.float32, 'lon_20m': np.float32, 'h_canopy_20m': np.float32,
                        'h_canopy':np.float32, 'h_te_best_fit_20m': np.float32,'h_te_best_fit': np.float32,
                        'sc_orient':np.uint8,'night_flag':np.int32,'layer_flag':np.uint8,'sat_flag':np.int16,
                        'dem_flag':np.uint8,'dem_removal_flag':np.uint8,'h_dif_ref': np.float32,
                        'terrain_flg':np.int32,'cloud_flag_atm':np.uint8,'h_te_std_20m':np.float32,
                        'ph_h_canopy':'object','d_flag_ca':'object','ph_h_tcanopy':'object','d_flag_tca':'object',
                        'n_ph_te_20m':np.uint16,'n_ph_20m':np.uint16,'beam_strength':'string','med_ht':np.float32,
                        'iqr_ht':np.float32,'p90_ht':np.float32,'p95_ht':np.float32,'bin0':np.uint16,'bin1':np.uint16,
                        'bin2':np.uint16,'bin3':np.uint16,'bin4':np.uint16,'start_dt':'datetime64[ns]','end_dt':'datetime64[ns]',
                        'tile':'object','year':np.uint16,'month':np.uint8}
            #print(df_final['dem_flag'].values)
            data = df_final[['lat_20m','lon_20m','h_canopy_20m','h_canopy','h_te_best_fit_20m',
                 'h_te_best_fit','sc_orient','night_flag','layer_flag','sat_flag','dem_flag',
                'dem_removal_flag','h_dif_ref','terrain_flg','cloud_flag_atm',
                'h_te_std_20m','ph_h_canopy','d_flag_ca','ph_h_tcanopy','d_flag_tca','n_ph_te_20m','n_ph_20m',
                'beam_strength','med_ht','iqr_ht','p90_ht','p95_ht','bin0','bin1',
                'bin2','bin3','bin4','start_dt','end_dt','tile','year','month']].astype(pd_dtype)

            ttprint(f'start saving {base_template}')

            #pq.write_to_dataset(pa.Table.from_pandas(data),
            #        f"/mnt/gaia/tmp/{bucket}/ATL08v006/test10",
            #        partition_cols=['tile','year','month'],
            #        existing_data_behavior='overwrite_or_ignore',
            #        basename_template=base_template,
            #        compression="snappy",
            #        version="2.4")
            #output_fn_file = f"{bucket}/ATL08v006/test.parquet"

            #pq.write_to_dataset(pa.Table.from_pandas(data),root_path=output_fn_file,
            #        partition_cols=['tile'],
            #        filesystem=httpfs,
            #        existing_data_behavior='delete_matching',
            #        compression="snappy",
            #        version="2.4")

            wr.s3.to_parquet(
                df=data,
                path=f's3://{s3_path}',
                dataset=True,
                filename_prefix=base_template,
                concurrent_partitioning=True,
                partition_cols=['tile', 'year', 'month'],
                boto3_session=sess)
            #try:
            #    data.to_parquet(s3_path, 
            #        compression='snappy', 
            # 	    engine = 'fastparquet',
            # 		open_with=myopen,
            #        append=True,
            #		partition_cols = ['tile', 'year', 'month'])
                    #print('start appending')
            #except FileNotFoundError:
            #        data.to_parquet(s3_path, 
            #        compression='snappy', 
            #        engine = 'fastparquet',
            #        open_with=myopen,
            #        partition_cols = ['tile', 'year', 'month'])   
                #df = spark.createDataFrame(data)
                #print(df)
                #df.write.mode("append").partitionBy('tile', 'year', 'month').parquet(s3_path_temporary)
                #pq.write_to_dataset(pa.Table.from_pandas(data),root_path=output_fn_file,
                #        partition_cols=['tile'],
                #        existing_data_behavior='overwrite_or_ignore',
                #        basename_template=base_template,
                #        compression="snappy",
                #        version="2.4")
                #ttprint(f'Copying {output_fn_file} to http://{host}/{bucket_name}/{output_fn_file}')
                #minio_client.fput_object(bucket_name, {output_fn_file}, output_fn_file)
                #os.remove(output_fn_file)

    except Exception as e:
        print(e)
        print(f'ERROR in {output_file}')
        #object_bucket = f'{bucket_name}'
        #object_name = f'{output_file}.pq'
        #ttprint(f'Copying {output_fn_file} to http://{host}/{object_bucket}/{out_pref}{object_name}')
        #minio_client.fput_object(object_bucket, f'{out_pref}{object_name}', output_fn_file)
        #os.remove(output_fn_file)




group = ['/gt1l', '/gt1r', '/gt2l', '/gt2r', '/gt3l', '/gt3r']
# Test file list
#file_list =['/mnt/gaia/raw/IceSAT/ATL08.006/2022.09.16/ATL08_20220916102706_13211608_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.10.14/ATL08_20221014210858_03681714_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.10.14/ATL08_20221014210858_03681714_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.10.15/ATL08_20221015090307_03761708_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.10.15/ATL08_20221015090307_03761708_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.11.13/ATL08_20221113073905_08181708_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2022.11.13/ATL08_20221113073905_08181708_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2023.02.12/ATL08_20230212031856_08181808_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2023.02.12/ATL08_20230212031856_08181808_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2023.05.13/ATL08_20230513110428_08101914_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2023.05.13/ATL08_20230513110428_08101914_006_01.h5',
#'/mnt/gaia/raw/IceSAT/ATL08.006/2023.05.13/ATL08_20230513225836_08181908_006_01.h5']
 

start_tile=int(sys.argv[1])
end_tile=int(sys.argv[2])
server_name=sys.argv[3]
g = group[start_tile]

folder = '/mnt/gaia/raw/IceSAT/ATL08.006/'
print('start')
args = []
for sub_dir in os.scandir(folder):
    for fname in os.scandir(sub_dir.path):
        args.append((fname.path,g))

print('file numbers',len(args))

#for fname in file_list:
#    output_file = fname.split('/')[-1][:-3]
#    for g in group:
#        args.append((fname,g))


### test
print('start processing files:', len(args))
#worker(args[3][0],args[3][1])


for result in parallel.job(worker, args, n_jobs=90):
    print(result)
