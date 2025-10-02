# GlobalEarthPoint

GlobalEarthPoint is an **Open Source Data Service Framework** that specializes in creating and accessing large geospatial point datasets. This repository serves two different groups: the ***Data Producers*** and the ***Data Users***.

For **Data Producers**, the adaptation of current GlobalEarthPoint’s workflow helps **create spatio-temporal partitions** for large data queries, **document metadata** and create **SpatioTemporal Asset Catalogs (STAC)**

For **Data Users**, we offer **STACs** below that serve as a visual portal for data query. In addition, the [snippet](https://colab.research.google.com/drive/1avMObD0G5KhOwK9Y8qRY2RVA7zKkPQ7c?usp=sharing) and online tutorial walk through the concept of cloud native vector format data queries (such as column selection, predicate pushdown, etc.).

In summary, this project supports creating *software-independent cloud-optimized* datasets. Once the data is created and hosted, a  Data User can  access it using any library that works with Parquet, such as [Polars](https://github.com/pola-rs/polars), [DuckDB](https://duckdb.org/), [Apache Arrow](https://arrow.apache.org/docs/index.html). It also provides tutorials to demonstrate finding, subsetting and retrieval of data efficiently from the cloud. The functionality is wrapped in both Python and R. Visit the notebooks linked below to explore the workflow.

## Key Features

*   **Data Producer**
    *   **High Efficiency**: Designed to handle massive datasets in [Parquet](https://parquet.apache.org/) format, minimizing data size, latency and maximizing throughput.
    *   **Cloud Optimization**: Data is stored using [GeoParquet](https://geoparquet.org/), an extension of [Arrow Parquet](https://arrow.apache.org/docs/python/parquet.html). The format benefits from a partitioning structure, and can be retrieved via Lazy evaluation.
*   **Data User**
    *   **Easy Integration**: Compatible with popular data processing frameworks and geospatial tools, facilitating easy integration into your existing workflows.
    *   **Advanced Query Capabilities**: Provides robust querying functionalities from [Polars](https://github.com/pola-rs/polars), [DuckDB](https://duckdb.org/), [Apache Arrow](https://arrow.apache.org/docs/index.html) to help you quickly extract meaningful insights from your data.
 
<p align="center">
    <img src="https://github.com/Open-Earth-Monitor/GlobalEarthPoint/blob/main/img/global-earth-point_framework.jpg" alt="workflow" width="600">
</p>

## Get Started

To get started with GlobalEarthPoint, check out our documentation below:

*   **Data Producer**

To replicate the creation of a large vector dataset follow the notebooks under `/GEDI02` or `/ICESat-2_ATL08_v6`. The process includes (1) **downloading raw data** via Data Producers (such as NASA, ESA, etc.), (2) **spatio-temporal** blocking, (3) aggregating and  calculating **metadata**, and (4) creating **STAC catalogues**. There is not a single workflow to modularize various sources of data processing, but to follow the framework and establish pipelines to process raw data individually. If you are interested in contributing more datasets or suggesting improvements of existing datasets, please feel free to raise an issue at [github.com/Open-Earth-Monitor/GlobalEarthPoint/issues](https://github.com/Open-Earth-Monitor/GlobalEarthPoint/issues).

*   **Data User**

See a list of currently available global point dataset below (*Update 24/09/2025*):

**Global Lidar #1: OpenLandMap Global Ecosystem Dynamics Investigation Level 2 fusion data (OLM GEDI02)**

*Data Catalogue:* [*OpenLandMap GEDI02*](https://stac.openlandmap.org/GEDI02/collection.json)

*Visit* [*this table*](https://github.com/Open-Earth-Monitor/GlobalEarthPoint/blob/main/GEDI02/olm_column_table.md) *for details of attributes.*

**Global Lidar #2: High-quality Ice, Cloud and land Elevation Satellite data (OLM ICESat-2 ATL08v6)**

*Data Catalogue:* [*OpenLandMap ICESat-2 ATL08 version 6*](https://stac.openlandmap.org/ICESat-2_ATL08v6/collection.json)

*Visit* [*this table*](https://github.com/Open-Earth-Monitor/GlobalEarthPoint/blob/main/ICESat-2_ATL08_v6/olm_column_table.md) *for details of attributes.*

To access the data via catalogue, here is an optimal snap code in Python to access the data. In addition, to learn more about cloud-native vector data accessing using different packages (such as [Polars](https://github.com/pola-rs/polars), [DuckDB](https://duckdb.org/), [Apache Arrow](https://arrow.apache.org/docs/index.html)), you can visit our [online tutorials](https://github.com/Open-Earth-Monitor/GlobalEarthPoint?tab=readme-ov-file#online-tutorials) also available in Python and R.

## Online Tutorials

1.  Video recorded lecture:
    *   Geo-Open-Hack 2024 - Accessing Big Vector Data on the Cloud using Arrow Parquet: [link](https://av.tib.eu/media/69559)

2.  Self-contained script:
    *   [Notebook: Access OpenLandMap GEDI/ICESat-2 via Cloud-native GeoParquet](https://colab.research.google.com/drive/13nSOejRLUyanwGtE6tWm_K9BqvVpDRVQ?usp=sharing)
    *   [Notebook: Overlay, filter, stratify OLM-GEDI for Canopy Height Modeling](https://colab.research.google.com/drive/1uJcGMzPANoCLyjpqfrNqke3SDUB2sqc_?usp=sharing)

*Note: scripts are contained in [Google Colab](https://colab.google/). To modify you will require to navigate* *File* *>* *Save a copy in Drive*

3.  External repository
    *   OGH summer school 2025: [link](https://github.com/yu-feng-ho/OGH2025)
    *   Other related toolbox: [gedidb](https://github.com/simonbesnard1/gedidb) developed and maintained by German Research Centre for Geosciences(GFZ).
  
## Acknowledgements & Funding

This work is supported by [OpenGeoHub Foundation](https://opengeohub.org/) and has received funding from the European Commission (EC) through the projects:

*  [Open-Earth-Monitor Cyberinfrastructure](https://earthmonitor.org/): Environmental information to support EU’s Green Deal (1 Jun. 2022 – 31 May 2026 - [101059548](https://cordis.europa.eu/project/id/101059548))
