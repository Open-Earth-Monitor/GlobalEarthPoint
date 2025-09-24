# Column Information of OpenLandMap ICESat-2 ATL08 version 6

ICESat-2 ATL08 version 6 columns are selected from:

1.  ATLAS/ICESat-3 L3A Land and Vegetation Height Version 6 (<https://nsidc.org/data/atl08/versions/6>) \[1\]

*Table: OpenLandMap ICESat-2 ATL08 version 6 Metadata, including resolution, if data is generated, data type and description.*
| **Variable**        | **Resolution** | **Generated** | **Data type** | **Description / Usage** |
|----------------------|----------------|---------------|---------------|--------------------------|
| lat_20m              | 20             | 0             | float32       | Center latitude of 20m geosegments within each 100m land segment |
| lon_20m              | 20             | 0             | float32       | Center longitude of 20m geosegments within each 100m land segment |
| h_canopy_20m         | 20             | 0             | float32       | 98% height of all the individual canopy relative heights for each 20m geosegment. If it is nodata, leave it empty |
| h_canopy             | 100            | 0             | float32       | 98% height of all the individual canopy relative heights |
| h_te_best_fit_20m    | 20             | 0             | float32       | Best fit terrain height to center of each 20m geosegment |
| h_te_best_fit        | 100            | 0             | float32       | The best fit terrain elevation at the mid-point location of each 100m segment |
| sc_orient            | 100            | 0             | byte          | Track of the spacecraft orientation between forward, backward and transitional flight modes |
| night_flag           | 100            | 0             | int32         | Flag indicating the data were acquired in night conditions: 0=day, 1=night. Derived from solar elevation at the geolocated segment |
| layer_flag           | 100            | 0             | byte          | Combination of multiple flags (cloud_flag_atm, cloud_flag_asr, and bsnow_con). Considers day/night. 1=clouds/snow likely, 0=absence |
| sat_flag             | 100            | 0             | byte          | — |
| dem_flag             | 100            | 0             | byte          | Indicates source of the DEM height. Values: 0=None, 1=Arctic, 2=Global, 3=MSS, 4=Antarctic |
| dem_removal_flag     | 100            | 0             | byte          | Flag indicating > dem_removal_percent_limit (default 20.0) removed from land segment due to failing DEM-QA tests |
| h_dif_ref            | 100            | 0             | float32       | Difference between h_te_median and ref_DEM |
| terrain_flg          | 100            | 0             | int32         | Terrain flag quality check: 1=exceeds threshold deviation from DEM, 0=otherwise |
| cloud_flag_atm       | 100            | 0             | byte          | Flag of cloud presence (0=no clouds detected) |
| h_te_std_20m         | 20             | 1             | float32       | Standard deviation of terrain photon heights above interpolated ground surface |
| ph_h_canopy          | Individual photon | 1          | str           | Height of photon canopy above interpolated ground surface classified as canopy. Values comma-separated in string |
| ph_h_tcanopy         | Individual photon | 1          | str           | Height of photon canopy above interpolated ground surface classified as top of canopy. Values comma-separated in string |
| n_ph_te_20m          | 20             | 1             | uint16        | Count of total number of terrain photons |
| n_ph_20m             | 20             | 1             | uint16        | Count of total number of all individual photons |
| beam_strength        | 100            | 1             | str           | Beam strength of shot (‘strong’, ‘weak’) |
| med_ht               | 20             | 1             | float32       | 0.5 quantile of height of vegetation photon above interpolated ground surface |
| iqr_ht               | 20             | 1             | float32       | Interquartile range (0.75–0.25) of vegetation photon heights above interpolated ground surface |
| p90_ht               | 20             | 1             | float32       | 0.9 quantile of vegetation photon height above interpolated ground surface |
| p95_ht               | 20             | 1             | float32       | 0.95 quantile of vegetation photon height above interpolated ground surface |
| bin0                 | 20             | 1             | uint16        | Number of vegetation photons <1m height above ground surface |
| bin1                 | 20             | 1             | uint16        | Number of vegetation photons 1–3m height above ground surface |
| bin2                 | 20             | 1             | uint16        | Number of vegetation photons 3–5m height above ground surface |
| bin3                 | 20             | 1             | uint16        | Number of vegetation photons 5–15m height above ground surface |
| bin4                 | 20             | 1             | uint16        | Number of vegetation photons >15m height above ground surface |
| start_dt             | 100            | 1             | datetime      | — |
| end_dt               | 100            | 1             | datetime      | — |




### Reference

*1. Neuenschwander, A. L., Pitts, K. L., Jelley, B. P., Robbins, J., Markel, J., Popescu, S. C., Nelson, R. F., Harding, D., Pederson, D., Klotz, B. & Sheridan, R. (2023). ATLAS/ICESat-2 L3A Land and Vegetation Height. (ATL08, Version 6). [Data Set]. Boulder, Colorado USA. NASA National Snow and Ice Data Center Distributed Active Archive Center. https://doi.org/10.5067/ATLAS/ATL08.006.

