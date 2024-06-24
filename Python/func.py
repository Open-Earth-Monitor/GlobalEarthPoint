from shapely.geometry import Point, Polygon, MultiPolygon, LineString
from pyarrow.dataset import dataset
from s3fs import S3FileSystem
import polars as pl
import pandas as pd
import numpy as np
from collections import defaultdict
from joblib import Parallel, delayed

# Convert from geometry to 1x1 degree tiles 
def geometry_to_tile_indices(geometry):
    # Convert different geometry types to bounding box
    if isinstance(geometry, Point):
        bbox = geometry.buffer(1e-10)  # Buffer by 1 degree for points
    elif isinstance(geometry, LineString):
        bbox = geometry.buffer(1e-10)  # Buffer by 1 degree for lines
    elif isinstance(geometry, (Polygon, MultiPolygon)):
        bbox = geometry.envelope  # Bounding box for polygons and multipolygons
    else:
        raise ValueError("Unsupported geometry type")

    # Extract bounding box coordinates
    xmin, ymin, xmax, ymax = bbox.bounds

    lat_indices = range(int(ymin), int(ymax) + 1)
    lon_indices = range(int(xmin), int(xmax) + 1)
    lat_indices_str = [f'{str(i).zfill(2)}N' if i>=0 else f'{str(-i).zfill(2)}S' for i in lat_indices]
    lon_indices_str = [f'{str(i).zfill(3)}E' if i>=0 else f'{str(-i).zfill(3)}W' for i in lon_indices]

    # Create a list of 1x1 degree lat lon index strings
    lat_lon_indices = [f"{lon}_{lat}" for lon in lon_indices_str for lat in lat_indices_str]

    return lat_lon_indices

# aggregating months throughout the whole year
def shorten_year_months(strings):
    year_month_dict = defaultdict(list)

    # Separate strings into year and month
    for s in strings:
        year, month = s.split('-')
        year_month_dict[int(year)].append(int(month))
    shortened_strings = []
    # Check if a year spans all months, and if so, concatenate into a single string
    for year, months in year_month_dict.items():
        if set(months) == set(range(1, 13)):
            shortened_strings.append(str(year))
        else:
            for month in months:
                shortened_strings.append(f'{str(year)}-{str(month).zfill(2)}')
    return shortened_strings



class gedil2:
    def __init__(self,geom,start_dt='2019-04-18',end_dt='2023-03-16',n_jobs=-5):
        self.object = 'gedi-ard/level2/gedi.l2v002_pnt_20190418_20230316_go_epsg.4326_v20231219.parquet'
        self.fs = S3FileSystem(
                      endpoint_url='https://s3.eu-central-1.wasabisys.com',
                      anon=True)
        self.geom = geom
        if start_dt != '2019-04-18' or end_dt != '2023-03-16':
            self.period_range = pd.period_range(start=start_dt, end=end_dt, freq='M')
        else:
            self.period_range = None
        self.n_jobs = n_jobs

    def _meta_reader(self,arg):
        try:
            if isinstance(arg,str):
                pyarrow_dataset = self._read_parititon(arg)
            else:
                pyarrow_dataset = self._read_parititon(*arg)
            df_default=self._gedi_read_default(pyarrow_dataset)
            row_counts=df_default.select((pl.col('rg'))).collect().shape[0]
            return df_default, row_counts
        except FileNotFoundError:
            return None
        
    def _read_parititon(self, tile, year_month=None):
        if year_month is not None:
            if '-' in year_month:
                year,month = list(map(lambda x:int(x), year_month.split('-'))) 
                subset_path = self.object + f'/tile={tile}/year={year}/month={month}'
            else:
                year = year_month
                subset_path = self.object + f'/tile={tile}/year={year}'

        else:
            subset_path = self.object + f'/tile={tile}'
        pyarrow_dataset = dataset(
        source = subset_path,
        format = 'parquet',
        filesystem=self.fs
        )

        return pyarrow_dataset
    
    def _gedi_read_default(self,pyarrow_dataset):
        # scale each column accroding to the converters using for preprocessing
        default_ds = pl.scan_pyarrow_dataset(pyarrow_dataset).with_columns((pl.col("rh100","rh99","rh98",'rh97',"rh95",'rh75','rh50','rh25','sensitivity',
                                                                         "rh100_a1","rh99_a1","rh98_a1","rh97_a1","rh95_a1",'rh75_a1','rh50_a1','rh25_a1',
                                                                         "rh100_a2","rh99_a2","rh98_a2","rh97_a2","rh95_a2",'rh75_a2','rh50_a2','rh25_a2',
                                                                         "rh100_a3","rh99_a3","rh98_a3","rh97_a3","rh95_a3",'rh75_a3','rh50_a3','rh25_a3',
                                                                         "rh100_a4","rh99_a4","rh98_a4","rh97_a4","rh95_a4",'rh75_a4','rh50_a4','rh25_a4',
                                                                         "rh100_a5","rh99_a5","rh98_a5","rh97_a5","rh95_a5",'rh75_a5','rh50_a5','rh25_a5',
                                                                         "rh100_a6","rh99_a6","rh98_a6","rh97_a6","rh95_a6",'rh75_a6','rh50_a6','rh25_a6',
                                                                         'sensitivity_a1','sensitivity_a2','sensitivity_a3','sensitivity_a4','sensitivity_a5','sensitivity_a6',
                                                                          "omega","pgap_theta","cover","rhog","rhov"
                                                                         )*0.0001,
                                                                   pl.col('elev_lowestmode',
                                                                         'elev_lowestmode_a1','elev_lowestmode_a2','elev_lowestmode_a3','elev_lowestmode_a4','elev_lowestmode_a5','elev_lowestmode_a6',
                                                                         'fhd_normal')*0.01,
                                                                   pl.col('rg','rv')*0.1,
                                                                   pl.col('pai')*0.001))
        return default_ds
    
    def _read_parallel(self,args,columns):
        # use pointers to work with prodefined matrix
        row_counts = 0
        df_list=[]
        for result in Parallel(n_jobs=self.n_jobs)(delayed(self._meta_reader)(i) for i in args):
            if result is not None:
                df_list.append((result[0],columns))
                row_counts+=result[1]
        mat = np.zeros((row_counts,91)) if columns=='*' else np.zeros((row_counts,len(columns))) 

        def worker(df,colums):
            arr = df.select(pl.col(columns)).collect().to_numpy()
            return arr 

        print(f'compiling data from {len(args)} paritions {row_counts} points, please wait............')
        pointer = 0
        for result in Parallel(n_jobs=self.n_jobs)(delayed(worker)(*i) for i in df_list):
            arr_slice = slice(pointer, pointer+len(result))
            mat[arr_slice,:] = result 
            pointer += len(result)
        
        return pd.DataFrame(mat) if columns=='*' else pd.DataFrame(mat,columns=columns)
    def _read_meta_parallel(self,args):
        # count row number to return the schema and counts of row
        print('counting row number')
        row_counts = 0        
        for result in Parallel(n_jobs=self.n_jobs)(delayed(self._meta_reader)(i) for i in args):
            if result is not None:
                row_counts+=result[1]        
        return {'schema':result[0].schema,'row counts':row_counts}
    
    def retrieve(self,columns="*"):
        tiles = geometry_to_tile_indices(self.geom)
        if self.period_range is not None:
            period_range=self.period_range.strftime('%Y-%m').tolist()
            time_blocks=shorten_year_months(period_range)
            args = []
            for tile in tiles:
                for year_month in time_blocks:
                    args.append((tile,year_month))
            if len(args)<5:
                df_list=[]
                for i,arg in enumerate(args):
                    df_list.append(_read_parititon(args).select(pl.col(columns)).collect().to_pandas())
                final_pandas_df=pd.concat(df_list)
            else:
                final_pandas_df=self._read_parallel(args,columns)

            return final_pandas_df
        
        final_pandas_df=self._read_parallel(tiles,columns)
        return final_pandas_df
    
    def scan(self):
        tiles = geometry_to_tile_indices(self.geom)
        df_list=[]
        if self.period_range is not None:
            period_range=self.period_range.strftime('%Y-%m').tolist()
            time_blocks=shorten_year_months(period_range)
            args = []
            for tile in tiles:
                for year_month in time_blocks:
                    args.append((tile,year_month))

            if len(args)<5:
                row_counts=0
                for arg in args:
                    df,count = _meta_reader(arg)
                    row_count += count 
                return {'schema':df.schema,'row counts':row_counts}
            else:
                return self._read_meta_parallel(args)

        return self._read_meta_parallel(tiles)