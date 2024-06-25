# R function reference

**`draw_bbox()`**

- *Description:* Interactively draw a bounding box.
- *Input:* none
- *Output:* `sf` bbox object with Coordinate Reference System

**`tile_query(x, years = 2019:2023)`**

- *Description:* Get tiles intersecting with bbox.
- *Input:* 
    - `x`: bbox/polygon (`sf`) or 2-column `data.frame` with names 'lon' and 'lat'
    - `years`: integer vector with length > 0 containing years of interest
- *Output:* Subset of tile lookup table (`sf`)

**`bbox_query(x, years = 2019:2023, columns = "all")`**

- *Description:* Create polars queries to pq files on S3. Note: complex queries may take a factor of 3 longer; selecting columns add to the processing time.
- *Input:* 
    - `x`: bbox/polygon (`sf`) or 2-column `data.frame` with names  'lon' and 'lat'
    - `years`: integer vector with length > 0 containing years of interest
    - `columns`: 'all' or 'reduced' or `character` vectore with specific names
- *Output:* List of queries ready to be collected (one per tile and year)

**`download_gedi(x, out.dir = NULL, cores = 1, progress = TRUE, require_confirmation = TRUE, timeout = 500)`**

- *Description:* Download all GEDI data requested in a bbox-query or tile-query. Data is written to disk in a specified directory and can be opened using `arrow::open_dataset(...) |> dplyr::collect()`.
- *Input:* 
    - `x`: a bbox-query or tile-query 
    - `out.dir`: target directory path 
    - `cores`: specify `cores > 1` for parallel processing (integer) 
    - `progress`: logical, show a progress bar be displayed? 
    - `require_confirmation`: logical, warn and require interactive confirmation for tile downloads with over 10 mio points. 
    - `timeout`: download time out in seconds 	
- *Output:* 

**`show_columns()`**

- *Description:* Show all 94 available GEDI data columns.
- *Input:* none
- *Output:* Table with GEDI variable names including their [description - unit - scale - data type - valid range - no data value - source (GEDI processing level)]
 	 	
**`rescale_gedi(x)`**

- *Description:* Apply scale factor and NoData masking values from the `show_columns()` table to the data
- *Input:* 
    - `x`: in-memory table read with `arrow::open_dataset() |> dplyr::collect()` 	
- *Output:* Table with rescaled columns
 	
**`make_sf(x)`**

- *Description:* Turn non-spatial table into sf object.
- *Input:* 
    - `x`: in-memory table with columns “longitude” and “latitude”
- *Output:* `sf` object with CRS WGS84 (EPSG:4