#=================================================================
# Functions to query and download GEDI data on S3
#=================================================================

# Install required packages if not already present ---------------

list.of.packages <- c("arrow", "polars", "dplyr", "sf", "mapview", "mapedit", 
                      "leaflet.extras", "purrr", "parallel", "pbmcapply")
if (Sys.info()$sysname == "Windows") list.of.packages = c(list.of.packages, 
                                                          "doParallel", "foreach")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages) > 0){
  message("Installing missing packages ", new.packages)
  install.packages(new.packages, dependencies = T)
} 

# Interactive selection map --------------------------------------

lookup = file.path(getwd(), "R/lookup.fgb")
if (!file.exists(lookup)){
  .lookup_table = sf::st_read('http://s3.eu-central-1.wasabisys.com/gedi-ard/gedi_lookup.fgb', quiet=T)
  sf::st_write(.lookup_table, lookup, quiet = T, delete_dsn = T)
} else {
  .lookup_table = sf::st_read(lookup, quiet=T) 
}
rm(lookup)

mapview::mapviewOptions(basemaps = c('OpenStreetMap', 'Esri.WorldImagery'),
                        legend.pos = "bottomright")
ltm = c(
  list("N pts [mio]" = .lookup_table[1:2,]),
  .lookup_table |> 
  dplyr::mutate(n_points=n_points/1e06) |> 
  split(.lookup_table$year)
)
.gedi_basemap = mapview::mapview(ltm, zcol = "n_points", 
                                at = 0:9, lwd=0, 
                                label = c(F, rep("n_points", 5)),
                                hide = c(F,F,T,T,T,T),
                                homebutton = rep(F, 6),
                                legend = c(T,F,F,F,F,F),
                                layer.name = "GEDI L2 Count", 
                                popup = rep(F, 6))
rm(ltm)
draw_bbox = function(){
  mapedit::drawFeatures(
    .gedi_basemap, title = "Draw a Bounding Box",
    editorOptions = list(
      position = "bottomleft", polylineOptions = F,
      polygonOptions = F, circleOptions = F,
      rectangleOptions = leaflet.extras::drawRectangleOptions(), 
      markerOptions = F, singleFeature = T,
      circleMarkerOptions = F, editOptions = FALSE))
}

# Crete queries ---------------------------------------------------

.five_deg = function(x){
  floor(x / 5)*5
}

#' x can be 
#' (1) sf bbox or polygon
#' (2) bbox as vector / list 
#' (3) data.frame / tibble with columns lat & lon for a list of (non-) adjacent tiles
tile_query = function(x, years = 2019:2023){  #, verbose = F
  if ("sf" %in% class(x)){
    stopifnot("The provided bbox should be in CRS EPSG:4326 (WGS84)." = sf::st_crs(x) == sf::st_crs(4326))
    # test for other geometry types than polygon
    tiles = .lookup_table |> sf::st_filter(x) 
    x_bb = sf::st_bbox(x) |> as.list()
  } else if ((!"sf" %in% class(x)) & all(names(x) %in% c("xmin", "ymin", "xmax", "ymax"))){
    x_bb = as.list(x)
    tiles = dplyr::filter(.lookup_table, 
                          dplyr::between(lon, .five_deg(x_bb$xmin), .five_deg(x_bb$xmax)),
                          dplyr::between(lat, .five_deg(x_bb$ymin), .five_deg(x_bb$ymax)))
  } else if (class(x) %in% c("tibble", "data.frame") & all(names(x) %in% c("lat","lon"))){ 
    
    x_expand = dplyr::mutate(x, lon = .five_deg(lon), lat = .five_deg(lat)) |> 
      dplyr::distinct()
    tiles = dplyr::inner_join(.lookup_table, x_expand, by = dplyr::join_by(lon, lat))
    x_bb = c(range(x$lon), range(x$lat))[c(1,3,2,4)] |> setNames(c("xmin", "ymin", "xmax", "ymax"))
  } else {
    stop("Could not derive a list of tiles from input x. Make sure it is an sf bbox/polygon in lat/lon or a data.frame with columns named lat & lon.")
  }
  
  tiles = dplyr::filter(tiles, year %in% years)
  #if (verbose) message("Selected ", nrow(tiles), " complete tiles within the bounding box.")
  attr(tiles, "bbox") = x_bb
  return(tiles)
}

.build_bbox_query = function(t, cols, bbox, filters = NULL){
  q = pl$scan_parquet(t)$
    filter(pl$col("longitude")$is_between(bbox$xmin, bbox$xmax),                 
           pl$col("latitude")$is_between(bbox$ymin, bbox$ymax)
    )
  if (!is.null(cols)) q = q$select(cols)
  #if (!is.null(filters))
  q
}

.reduced_columns = readLines("R/reduced_columns.txt")

#' columns can be "default", "all", or character vector
bbox_query = function(x, years = 2019:2023, columns = "all"){  #, filters = NULL
  tls = tile_query(x, year = years)
  bbox = attr(tls, "bbox") |> as.list()
  b = "https://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614"
  urls = paste0(b, tls$dir)
  nms = paste0("GEDI_tile_subset", gsub("/", "_", dirname(tls$dir)))
  
  if (columns == "all") {
    cols = NULL
  } else if (columns == "reduced"){
    cols = .reduced_columns
  } else if (length(columns) > 1){
    cols = columns 
  }  
  queries = purrr::map(urls, .build_bbox_query, cols, bbox)
  names(queries) = nms
  return(queries)
}

# Download -------------------------------------------------------

.download_tile = function(i, urls, nms, dir){
  download.file(urls[i], file.path(dir, nms[i]))
}

.download_tile_windows_parellel = function(urls, nms, dir, cores){
  doParallel::registerDoParallel(cl <- doParallel::makeCluster(cores))
  foreach::foreach(i = 1:length(nms)) %dopar% {
    withCallingHandlers({ 
      opts = options()
      options(timeout = timeout)
      download.file(urls[i], file.path(dir, nms[i]))
      options(opts)
    }, error = function(e) stop("timeout"))
  }
  doParallel::stopCluster(cl)
}
  

download_gedi = function(x, out.dir = NULL, cores = 1, progress = T, 
                         require_confirmation = T, timeout = 500){
  if (is.null(out.dir)) out.dir = file.path(getwd(), "GEDI_download")
  if (!dir.exists(out.dir)) dir.create(out.dir, recursive = T)
  t = NULL
  # download entire tiles (x = table)
  if (all(c("lon", "lat", "year", "dir") %in% names(x))){
    n = sum(x$n_points)
    answ = "y"
    if (require_confirmation & n > 1e07){
      answ = as.character(readline(prompt = paste("You are about to download", nrow(x), 
                                              "tiles with", round(n/1e06,1), "million points. Do you want to continue? (y/n)")))
    }
    if (tolower(answ) %in% c("", "y")){
      message("Downloading tiles...")
      # construct URLs and filenames
      b = "https://s3.eu-central-1.wasabisys.com/gedi-ard/level2/l2v002.gedi_20190418_20230316_go_epsg.4326_v20240614"
      urls = paste0(b, x$dir)
      nms = paste0("GEDI_tile", gsub("/", "_", dirname(x$dir)), ".parquet")
      backup_options = options()
      options(timeout = timeout)
      t = system.time({
        if (cores==1){
          f = purrr::map(1:length(urls), .download_tile, urls = urls, nms = nms,
                         dir = out.dir, .progress = progress)
        } else if (progress) {
          f = pbmcapply::pbmclapply(1:length(urls), .download_tile, urls = urls,
                                    nms = nms, dir = out.dir, mc.cores = cores)
        } else {
          if (Sys.info()$sysname == "Windows"){
            f = .download_tile_windows_parellel(urls, nms, out.dir, cores)
          }
          f = parallel::mclapply(1:length(urls), .download_tile, urls = urls,
                                 nms = nms, dir = out.dir, mc.cores = cores)
        }
        f = unlist(f)
        options(backup_options)
      })
    } else {
      message("Download aborted.")
      #out.dir = ""
    }
  }
  # download partial tiles (queries)
  if (all(class(x) == "list" & class(x[[1]]) == "RPolarsLazyFrame")){
    message("Downloading bbox subsets...")
    nms = file.path(out.dir, paste0(names(x), ".parquet"))
    t = system.time({
      purrr::map2(x, nms, \(x,n) x$collect()$write_parquet(n), .progress = progress)
    })
  }
  if (!is.null(t)) message("Completed after ", round(t["elapsed"]), " sec.")
}

# Miscellaneous -------------------------------------------------

make_sf = function(data){
  sf::st_as_sf(data, 
               coords=c("longitude","latitude"), 
               crs=sf::st_crs(4326))
}

show_gedi_columns = function(){
  read.csv("R/gedi_columns.csv") |> 
    dplyr::as_tibble()
}

.rescale = function(n, x, scl){
  fac = scl[scl$Variable == n,]
  if (!is.na(fac$NoData)) x[[n]] = dplyr::na_if(x[[n]], as.integer(fac$NoData))
  if (fac$Scale < 1) x[[n]] = x[[n]] * fac$Scale
  return(x[n])
}
  
rescale_gedi = function(x){
  # download if not exists?
  scl = show_gedi_columns() |> 
    dplyr::select(Variable, Scale, NoData) |> 
    dplyr::filter(Variable %in% names(x))
  purrr::map(names(x), .rescale, x = x, scl = scl) |> 
    purrr::list_cbind()
}


 
