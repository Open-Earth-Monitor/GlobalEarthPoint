# GlobalEarthPoint

GlobalEarthPoint is a **Open Source Data Service Library** that specializes in accessing large geospatial point datasets. It provides tools to find, subset and retrieve data efficiently from the cloud. The functionality is wrapped in both Python and R. Visit the notebooks linked below to explore the workflow.

## Key Features

- **Cloud Optimization**: Data is stored using [Arrow Parquet](https://arrow.apache.org/docs/python/parquet.html) in the partitioning structure, and retrieved with Lazy evaluation. 
- **High Efficiency**: Designed to handle massive datasets in [Parquet](https://parquet.apache.org/) format, minimizing data size, latency and maximizing throughput.
- **Easy Integration**: Compatible with popular data processing frameworks and geospatial tools, facilitating easy integration into your existing workflows.
- **Advanced Query Capabilities**: Provides robust querying functionality from [Polars](https://github.com/pola-rs/polars) to help you quickly extract meaningful insights from your data.


## Get Started

To get started with GlobalEarthPoint, check out our documentation and tutorial in Python and R. See a list of currently available global point dataset below:

### Global Lidar #1: High-quality Global Ecosystem Dynamics Investigation (GEDI) data

*R*

- [R notebook](R/R_GEDI_Access_S3.md)
- [R functions documentation](R/R_reference.md)

*Python*

- [Python notebook](Python/Python_GEDI_Access_S3.ipynb)

### Global Lidar #2: High-quality Ice, Cloud and land Elevation Satellite data (ICE-Sat2; coming soon!)