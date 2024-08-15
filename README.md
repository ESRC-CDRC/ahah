<div align="center">

# Access to Healthy Assets & Hazards (AHAH)

**Road network routing between postcodes and health related POIs using NetworkX**

<a href="https://www.python.org"><img alt="Python" src="https://img.shields.io/badge/python%20-%2314354C.svg?&style=for-the-badge&logo=python&logoColor=white"/></a>
<a href="https://rapids.ai/"><img alt="RAPIDS" src="https://img.shields.io/badge/-rapids.ai-blueviolet?style=for-the-badge"></a>  
<a href="https://dvc.org/"><img alt="DVC" src="https://img.shields.io/badge/data-DVC-lightblue?style=flat-square"></a>
<a href="https://black.readthedocs.io/en/stable/"><img alt="Code style: black" src="https://img.shields.io/badge/style-black-000000.svg?style=flat-square"></a>

</div>

<p align="center">
<a href="https://cjber.github.io/ahah/">Documentation</a>
</p>

[Cillian
Berragan](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@cjberragan`](http://twitter.com/cjberragan)\]<sup>1\*</sup> [Mark
Green](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@markalangreen`](http://twitter.com/markalangreen)\]<sup>1</sup>
[Alex
Singleton](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@alexsingleton`](http://twitter.com/alexsingleton)\]<sup>1</sup>

<sup>1</sup> _Geographic Data Science Lab, University of Liverpool,
Liverpool, United Kingdom_  
<sup>\*</sup> _Correspondence_: C.Berragan@liverpool.ac.uk

## Overview

This project identifies the time-weighted distance required to travel by road between every postcode in Great Britain and a selection of health related points of interest. A ranked combination of these drive-times is used to create the AHAH index.

Access is defined through the average time-weighted road network distance for each postcode within each LSOA to the nearest point of interest of a particular type. For this, the road highways network and road speed estimates provided through [Ordnance Survey](https://www.ordnancesurvey.co.uk/business-government/products/open-map-roads) was used, alongside the [ONS Postcode Directory](https://geoportal.statistics.gov.uk/search?q=PRD_ONSPD&sort=Date%20Created%7Ccreated%7Cdesc), which gives centroids for every postcode in the country.

This is a computationally intense calculation, with the total road network used having ~3.8 million edges, and \~3.2million nodes. Access to each nearest health related POI was calculated using the _Multi Source Shortest Path_ algorithm, for all ~1.7 million postcodes in Great Britain.

# Project layout

```bash
ahah
├── aggregate_lsoa.py  # aggregate outputs to LSOA level
├── create_index.py  # use aggregates to create index
├── os_highways.py  # process OS open roads data
├── air_lsoa.py  # process air quality data
├── preprocess.py  # process all POI data
├── routing.py  # main routing script
└── common
    └── utils.py  # utility functions
```

## Methodology

Accessibility measures were created using the `networkx` Python library in conjunction with the OS Open Road network. Unlike similar routing software like Routino, which uses Open Street Map data, the OS Open Road Network provides more accurate road speed estimates for UK roads.

In this study, we measured the network distance (driving travel time) between the centroid of each active postcode in Great Britain to the coordinates of each unique health asset (e.g. GP practice). Measured network distances for each indicator for postcodes were aggregated to the LSOA level, providing average network distance for each indicator (as a measure of accessibility). All other indicators were also summarised for LSOAs. The indicators within each domain were standardised by ranking and transformed to the standard normal distribution. The direction of each variable was dictated by the literature (e.g. accessibility to fast food outlets were identified as health negating, whereas accessibility to GP practices was health promoting).

To calculate our overall index (and domain specific values), we followed the methodology of the 2015 IMD. For each domain, we ranked each domain $R$ and any LSOA scaled to the range $[0,1]$. $R=1/N$ for the most 'health promoting' LSOA and $R=N/N$ for the least promoting, where $N$ is the number of LSOAs in Great Britain. Exponential transformation of the ranked domainscores was then applied to LSOA values to reduce ‘cancellation effects’. So, for example, high levels of accessibility in one domainare not completely cancelled out by low levels of accessibility in a different domain. The exponential  transformation  applied also puts  more  emphasis  on  the LSOAs  at  theend  of  the health demoting side of the distribution and so facilitates identification of the neighbourhoods with the worsthealth promoting aspects. The exponential transformed indicator score $X$ is given by:

$$
X=−23ln(1−R(1−exp^{−100/23}))
$$

where ‘ln’ denotes natural logarithm and ‘exp’ the exponential transformation.

The main domains across our  indicators: retail  services,  health  services, physical  environment and  air quality then were combined to form an overall index of‘Access to Healthy Assets and Hazards’ (AHAH)

<div style="text-align: center;">

![](./overview.png)

</div>

## Methods

Preprocessing of the OS Open Road network is performed within the [UKRoutes](https://github.com/cjber/ukroutes) Python library. The main `Route` class is defined by this library; using the `multi_source_dijkstra_path_length` function from NetworkX.

Please see the [UKRoutes methods](https://github.com/cjber/ukroutes?tab=readme-ov-file#routing-methodology) for more information.

### 2. Process Data `ahah/preprocess.py`

- Clean raw data
- Save to parquet files

### 3. Routing `ahah/routing.py`

- Iterate over every processed Parquet file in the `data/processed` directory 
- Use `Route` class from `UKRoutes` to route from POIs to postcodes
- Write to `data/out` directory

### 4. Process air quality data `ahah/process_air.py`

- Create raster of interpolated values from monitoring station points
  - Exclude points that are _MISSING_
- Aggregate to LSOA by taking mean values

### 5. Combine into index `ahah/create_index.py`

- Combine both processed secure and open data
- Intermediate variables calculated
  - All variables ranked
  - Exponential default calculated for all ranked variables
  - Percentiles calculated from ranked variables
- Domains Scores calculated
  - Domain scores calculated from mean of each domains input variables
  - Domain scores ranked
  - Domain percentiles calculated
  - Exponential transformation calculated for each domain
- AHAH index calculated from mean of domain exponential transformations
  - Ranked AHAH index calculated
  - AHAH percentiles calculated

## AHAH Data Sources

See the [AHAH V2 FigShare Repository](https://figshare.com/articles/online_resource/Access_to_Healthy_Assets_and_Hazards_AHAH_-_Updated_version_2017/8295842/1) for the previous iteration.

See [DATA.md](reports/DATA.md) for current data sources.
