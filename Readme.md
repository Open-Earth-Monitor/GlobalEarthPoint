# GlobalEarthPoint

GlobalEarthPoint is a **Open Source Data Service Library** that specializes in accessing large geospatial point datasets. This project is a *Software Free* library that interacts with any possible libaries that work with Parquet, such as [Polars](https://github.com/pola-rs/polars), [DuckDB](https://duckdb.org/), [Apache Arrow](https://arrow.apache.org/docs/index.html). It also provides tutorials to demonstrate finding, subseting and retrieval of data efficiently from the cloud. The functionality is wrapped in both Python and R. Visit the notebooks linked below to explore the workflow. 

## Key Features

- **Cloud Optimization**: Data is stored using [GeParquet](https://geoparquet.org/), an extension of [Arrow Parquet](https://arrow.apache.org/docs/python/parquet.html). The format features in the partitioning structure, and retrieved with Lazy evaluation. 
- **High Efficiency**: Designed to handle massive datasets in [Parquet](https://parquet.apache.org/) format, minimizing data size, latency and maximizing throughput.
- **Easy Integration**: Compatible with popular data processing frameworks and geospatial tools, facilitating easy integration into your existing workflows.
- **Advanced Query Capabilities**: Provides robust querying functionalities from [Polars](https://github.com/pola-rs/polars), [DuckDB](https://duckdb.org/), [Apache Arrow](https://arrow.apache.org/docs/index.html) to help you quickly extract meaningful insights from your data.


## Get Started

To get started with GlobalEarthPoint, check out our documentation and [Online tutorial](https://github.com/Open-Earth-Monitor/GlobalEarthPoint?tab=readme-ov-file#online-tutorials) in Python and R. See a list of currently available global point dataset below:

### Global Lidar #1: OpenLandMap Global Ecosystem Dynamics Investigation Level 2 fusion data (OLM GEDI02) 

*Data Catalogue: [OpenLandMap GEDI02](https://stac.openlandmap.org/GEDI02/collection.json)*

### Global Lidar #2: High-quality Ice, Cloud and land Elevation Satellite data (OLM ICESat-2 ATL08v6)

*Data Catalogue: [OpenLandMap ICESat-2 ATL08 version 6](https://stac.openlandmap.org/ICESat-2_ATL08v6/collection.json
)*

## Online Tutorials: 

1. Video recorded lecture:
- Geo-Open-Hack 2024 - 	
Accessing Big Vector Data on the Cloud using Arrow Parquet: [link](https://av.tib.eu/media/69559)

2. Self-contained script:
- [Notebook: Access OpenLandMap GEDI/ICESat-2 via Cloud-native GeoParquet](https://colab.research.google.com/drive/13nSOejRLUyanwGtE6tWm_K9BqvVpDRVQ?usp=sharing)
- [Notebook: Overlay, filter, stratify OLM-GEDI for Canopy Height Modeling](https://colab.research.google.com/drive/1uJcGMzPANoCLyjpqfrNqke3SDUB2sqc_?usp=sharing)
  
*Note: scripts are contained in [Google Colab](https://colab.google/). To modify you will require to navigate `File` > `Save a copy in Drive`*

3. External respository
- OGH summer school 2025: [link](https://github.com/yu-feng-ho/OGH2025)
- Other related toolbox: [gedidb](https://github.com/simonbesnard1/gedidb) developed and maintained by German Research Centre for Geosciences(GFZ). 

## Acknowledgements & Funding

This work is supported by OpenGeoHub Foundation, University of Münster, International Institute for Applied Systems Analysis (IIASA), and has received funding from the European Commission (EC) through the projects:
- [Open-Earth-Monitor Cyberinfrastructure](https://earthmonitor.org/): Environmental information to support EU’s Green Deal (1 Jun. 2022 – 31 May 2026 - 101059548)
