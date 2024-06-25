import os
os.environ['USE_PYGEOS'] = '0'
from pyarrow.dataset import dataset
import polars as pl
import pyarrow.fs as fs
import geopandas as gpd
import json
import contextily as cx
from shapely.geometry import shape
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from joblib import Parallel, delayed
import leafmap.foliumap as leafmap
from urllib.parse import urlparse
import math
import time
import contextlib
import joblib
from tqdm import tqdm

@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()

        
    
#!pip install ipywidgets
#!pip install jupyter-leaflet
#!pip install --upgrade ipywidgets jupyter-leaflet
#!jupyter nbextension enable --py widgetsnbextension --sys-prefix
#!jupyter nbextension enable --py --sys-prefix ipyleaflet
#!jupyter labextension install @jupyter-widgets/jupyterlab-manager


def mapview(lookup_file='lookup.fgb'):
    #=================================================================
    # Interactive map serving as an overview for Global Point Data
    #=================================================================
    
    # Define the path for the lookup file
    lookup = os.path.join(os.getcwd(), lookup_file)

    # Check if the lookup file exists
    if not os.path.exists(lookup):
        # Read the lookup table from the URL
        lookup_table = gpd.read_file(lookup_table_url)
        lookup_table['url'] = 'http://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614' + lookup_table['dir'] 
        # Write the lookup table to a file
        lookup_table.to_file(lookup, driver='FlatGeobuf')
    else:
        # Read the lookup table from the local file
        lookup_table = gpd.read_file(lookup)

    # Configure map options
    basemaps = {
        'Esri.WorldImagery': 'Esri.WorldImagery'
    }

    # Process the lookup table
    lookup_table['n_points_mio'] = lookup_table['n_points'] / 1e06
    lookup_table['year'] = lookup_table['year'].apply(lambda x: int(x))
    ltm = {'N pts [mio]': lookup_table.iloc[1:2]}
    ltm.update({year: group for year, group in lookup_table.groupby('year')})

    # Create the base map
    m = leafmap.Map(center=[0, 0], zoom=2)
    for name, basemap in basemaps.items():
        m.add_basemap(basemap)

    # Add the layers to the map with customized colors
    for year, data in ltm.items():
        m.add_data(data, layer_name=f"Year {year}", column='n_points_mio', cmap='YlGn', legend_title='Number of Points (million)')

    return m



class gedil2:	
    #=================================================================
    # Class of GEDI Level2 to query and download GEDI data on S3
    #=================================================================

    def __init__(self, geometry=None, years=[2019,2020,2021,2022,2023]):
        self.url_dataset = "https://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614"
        self.geometry = geometry
        self.years = years
    
    def _build_bbox_query(self, url, cols, bbox):   
        q =  pl.scan_parquet(url)
        if cols is not None:
            q = q.select(cols)
        q = q.filter((pl.col('longitude') >= bbox[0]) & (pl.col('longitude') <= bbox[2]) &
                     (pl.col('latitude') >= bbox[1]) & (pl.col('latitude') <= bbox[3]))
        return q
    
    def _download_tile(self, url, filename, out_dir):
        response = requests.get(url, stream=True)
        filepath = os.path.join(out_dir, filename)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath 
    
    def bbox_query(self,columns='reduced'):
        with open('reduced_columns.txt') as f:
            reduced_columns = [i.replace('\n','') for i in f.readlines()]

        if columns == "all":
            cols = None
        elif columns == "reduced":
            cols = reduced_columns
        else:
            cols = columns

        tls = self.tile_query()
        bbox = tls.bbox
        urls = [f"{self.url_dataset}{dir}" for dir in tls['dir']]
        nms = [f"GEDI_tile_subset{urlparse(dir).path.replace('/', '_')}" for dir in tls['dir']]

        queries = [self._build_bbox_query(url, cols, bbox) for url in urls]
        queries_dict = dict(zip(nms, queries))
        return queries_dict
    
    def tile_query(self):
        lookup_table = gpd.read_file('lookup.fgb')  
        # input as geopandas dataframe

        tiles = lookup_table[lookup_table.intersects(self.geometry)]
        tiles = tiles[tiles['year'].isin(self.years)]

        x_bb = self.geometry.bounds
        tiles.bbox = x_bb
        return tiles

    def show_gedi_columns(self):
        return pd.read_csv("gedi_columns.csv") 

    
    def download_gedi(self, x, out_dir=None, cores=1, progress=True, require_confirmation=True):
        
        if out_dir is None:
            out_dir = os.path.join(os.getcwd(), "GEDI_download")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        t = time.time()
        if isinstance(x, gpd.GeoDataFrame):
            if all(col in x.columns for col in ["lon", "lat", "year", "dir"]):
                n = x['n_points'].sum()
                answ = 'y'
                if require_confirmation and n > 1e7:
                    answ = input(f"You are about to download {len(x)} tiles with {n / 1e6:.1f} million points. Do you want to continue? (y/n): ")
                if answ.lower() in ('', 'y'):
                    print("Downloading tiles...")
                    urls = [self.url_dataset + dir_path for dir_path in x['dir']]
                    filenames = [f"GEDI{os.path.basename(dir_path.replace('/','_'))}" for dir_path in x['dir']]
                    if cores == 1:
                        for url, filename in tqdm(zip(urls, filenames), desc="Downloading tiles"):
                            print(url,filename)
                            self._download_tile(url, filename, out_dir)
                    else:
                        args = []
                        for url, filename in zip(urls, filenames):
                            args.append([url, filename])
                        if progress:
                            with tqdm_joblib(tqdm(desc="Downloading tiles", total=cores)) as progress_bar:  
                                Parallel(n_jobs=cores)(delayed(self._download_tile)(i[0],i[1],out_dir) for i in args)
                                                  
                else:
                    print("Download aborted.")
                    out_dir = ""
        elif isinstance(x, dict):
            print("Downloading bbox subsets...")
            def sink_parallel(lazyframe,filename):
                lazyframe.sink_parquet(filename)
                
            if cores > 1:
                args = []
                for filename,item in x.items():
                    args.append((item,filename))
                if progress:
                    with tqdm_joblib(tqdm(desc="Downloading subset", total=cores)) as progress_bar:        
                        Parallel(n_jobs=cores)(delayed(sink_parallel)(i[0],f'{out_dir}/{i[1]}') for i in args)
            else:
                for filename,item in tqdm(x.items(), desc="Downloading subset"):
                    item.sink_parquet(f"{out_dir}/{filename}")
        elapsed_time = time.time() - t
        print(f"Completed after {elapsed_time:.2f} sec.")