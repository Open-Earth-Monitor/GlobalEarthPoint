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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import leafmap.foliumap as leafmap
from urllib.parse import urlparse
import time

#!pip install ipywidgets
#!pip install jupyter-leaflet
#!pip install --upgrade ipywidgets jupyter-leaflet
#!jupyter nbextension enable --py widgetsnbextension --sys-prefix
#!jupyter nbextension enable --py --sys-prefix ipyleaflet
#!jupyter labextension install @jupyter-widgets/jupyterlab-manager


def mapview():
    # Define the path for the lookup file
    lookup = os.path.join(os.getcwd(), "lookup.fgb")

    # Check if the lookup file exists
    if not os.path.exists(lookup):
        # Read the lookup table from the URL
        lookup_table = gpd.read_file('http://s3.eu-central-1.wasabisys.com/gedi-ard/gedi_lookup.fgb')
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
    m = leafmap.Map(center=[0, 0], zoom=1)
    for name, basemap in basemaps.items():
        m.add_basemap(basemap)

    # Add the layers to the map with customized colors
    for year, data in ltm.items():
        m.add_data(data, layer_name=f"Year {year}", column='n_points_mio', cmap='YlGn', legend_title='Number of Points (million)')

    return m


class gedil2:	
    def __init__(self, geometry=None, years=[2019,2020,2021], columns = ['latitude','longitude','elev_lowestmode']):
        self.url_dataset = "https://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614"
        self.geometry = geometry
        self.years = years

        if columns == "all":
            self.cols = None
        elif columns == "reduced":
            self.cols = reduced_columns
        else:
            self.cols = columns

    
    def _build_bbox_query(self, url, cols, bbox):    
        q =  pl.scan_parquet(url)
        if cols is not None:
            q = q.select(cols)
        q = q.filter((pl.col('longitude') >= bbox[0]) & (pl.col('longitude') <= bbox[2]) &
                     (pl.col('latitude') >= bbox[1]) & (pl.col('latitude') <= bbox[3]))
        return q
    
    def _download_tile(url, filename, out_dir):
        response = requests.get(url, stream=True)
        filepath = os.path.join(out_dir, filename)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath 
    
    def bbox_query(self):

        tls = self.tile_query()
        bbox = tls.bbox
        urls = [f"{self.url_dataset}{dir}" for dir in tls['dir']]
        nms = [f"GEDI_tile_subset{urlparse(dir).path.replace('/', '_')}" for dir in tls['dir']]

        queries = [self._build_bbox_query(url, self.cols, bbox) for url in urls]
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

    def show_columns(self):
        lookup_table = gpd.read_file('lookup.fgb')  
        # input as geopandas dataframe

        tiles = lookup_table[lookup_table.intersects(self.geometry)]
        tiles = tiles[tiles['year'].isin(self.years)]

        x_bb = self.geometry.bounds
        tiles.bbox = x_bb
        return tiles

    def download_gedi(self, x, out_dir=None, cores=1, progress=True, require_confirmation=True):
        
        if out_dir is None:
            out_dir = os.path.join(os.getcwd(), "GEDI_download")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
            
        if isinstance(x, gpd.GeoDataFrame):
            if all(col in x.columns for col in ["lon", "lat", "year", "dir"]):
                n = x['n_points'].sum()
                answ = 'y'
                if require_confirmation and n > 1e7:
                    answ = input(f"You are about to download {len(x)} tiles with {n / 1e6:.1f} million points. Do you want to continue? (y/n): ")

                if answ.lower() in ('', 'y'):
                    print("Downloading tiles...")
                    base_url = "https://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614"
                    urls = [base_url + dir_path for dir_path in x['dir']]
                    filenames = [f"GEDI{os.path.basename(dir_path.replace('/','_'))}.parquet" for dir_path in x['dir']]

                    if cores == 1:
                        futures = []
                        for url, filename in zip(urls, filenames):
                            futures.append(self._download_tile(url, filename, out_dir))
                        t = time.time()
                        for future in tqdm(futures, desc="Downloading tiles"):
                            future.result()
                    else:
                        with (ProcessPoolExecutor(max_workers=cores) if cores > 1 else ThreadPoolExecutor(max_workers=cores)) as executor:
                            futures = [executor.submit(self._download_tile, url, filename, out_dir) for url, filename in zip(urls, filenames)]
                            t = time.time()
                            if progress:
                                for future in tqdm(futures, desc="Downloading tiles"):
                                    future.result()

                    elapsed_time = time.time() - t
                    print(f"Completed after {elapsed_time:.2f} sec.")
                else:
                    print("Download aborted.")
                    out_dir = ""
        elif isinstance(x, dict):
            print("Downloading bbox subsets...")
            filenames = [os.path.join(out_dir, f"{name}.parquet") for name in x.keys()]
            t = time.time()
            if cores > 1:
                with ProcessPoolExecutor(max_workers=cores) as executor:
                    futures = [executor.submit(item.to_pandas().to_parquet, filename) for item, filename in zip(x, filenames)]
                    for future in tqdm(futures, desc="Downloading subsets"):
                        future.result()
            else:
                for item, filename in zip(x, filenames):
                    x[item].sink_parquet(filename)
            elapsed_time = time.time() - t
            print(f"Completed after {elapsed_time:.2f} sec.") 