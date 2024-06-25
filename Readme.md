<<<<<<< Updated upstream
# GlobalEarthPoint
=======
# Global Earth Point

This repository contains software to access larger-than-memory geospatial vector data (currently only GEDI Level 2A & B) in the cloud. These tools build upon Apache Arrow and Polars technologies to facilitate lazy loading and efficient selection of data of interest. The functionality is wrapped in both Python and R. Visit the notebooks linked below to explore the workflow.
>>>>>>> Stashed changes

GlobalEarthPoint is a **Open Source Data Service Library** that specializes in managing large datasets of geographical points, providing a portal to access cloud infrasturcture, subset and retrieve data efficiently. 

### Key Features

- **Cloud Optimization**: Data is stored using [ArrowParquet](https://arrow.apache.org/docs/python/parquet.html) in the partitioning structure, and retrieved with Lazy evaluaton. 
- **High Efficiency**: Designed to handle massive datasets by [Parquet](https://parquet.apache.org/) format, minimizing data size, latency and maximizing throughput.
- **Easy Integration**: Compatible with popular data processing frameworks and geospatial tools, facilitating easy integration into your existing workflows.
- **Advanced Query Capabilities**: Provides robust querying functionality from [Polars](https://github.com/pola-rs/polars) to help you quickly extract meaningful insights from your data.


### Get Started

To get started with GlobalEarthPoint, check out our documentation and tutorial in Python and R. List of global point data is currently avaiable online (see below):

#### Global Lidar #1: high-quality Global Ecosystem Dynamics Investigation (GEDI) 

##### R

- [R notebook](R/R_GEDI_Access_S3.md)
- [R functions documentation](R/R_readme.md)

#### Python

- [Python notebook](Python/Python_GEDI_Access_S3.ipynb)
