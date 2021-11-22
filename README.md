<div align="center">

# Access to Healthy Assets & Hazards (AHAH)

**GPU accelerated road network routing between postcodes and POIs.**

<a href="https://www.python.org"><img alt="Python" src="https://img.shields.io/badge/python%20-%2314354C.svg?&style=for-the-badge&logo=python&logoColor=white"/></a>
<a href="https://rapids.ai/"><img alt="RAPIDS" src="https://img.shields.io/badge/-rapids.ai-blueviolet?style=for-the-badge"></a>

</div>

<p align="center">
<a href="https://cjber.github.io/ahah/ahah">Documentation</a> •
<a href="todo">FigShare (soon)</a>
</p>

[Cillian
Berragan](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@cjberragan`](http://twitter.com/cjberragan)\]<sup>1\*</sup> [Mark
Green](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@markalangreen`](http://twitter.com/markalangreen)\]<sup>1</sup>
[Alex
Singleton](https://www.liverpool.ac.uk/geographic-data-science/our-people/)
\[[`@alexsingleton`](http://twitter.com/alexsingleton)\]<sup>1</sup>

<sup>1</sup> *Geographic Data Science Lab, University of Liverpool,
Liverpool, United Kingdom*  
<sup>\*</sup> *Correspondence*: C.Berragan@liverpool.ac.uk

## Overview

This project identifies the distance required to travel by road between
every postcode in England and a selection of health related points of
interest. A ranked combination of these drive-times is used to create
the AHAH index.

Access is defined through the average time-weighted road network
distance for each postcode within each LSOA to the nearest point of
interest of a particular type. For this, the road highways network and
road speed estimates provided through [Ordnance
Survey](https://www.ordnancesurvey.co.uk/business-government/products/mastermap-highways)
was used, alongside the [ONS Postcode Directory for May
2020](https://data.gov.uk/dataset/06803af0-6054-410a-822a-f7ab30bcd8b1/ons-postcode-directory-may-2020),
which gives centroids for every postcode in the country.

This is a very computationally intense calculation, with the total road
network used having 5,062,741 edges, and 4,289,045 nodes. The
single-source shortest path algorithm was used to determine the
time-weighted network distance from all 1,463,696 postcodes in England
to their nearest vaccination site.

This calculation was made possible through the GPU accelerated Python
library `cugraph`, part of the [NVIDIA RAPIDS
ecosystem](https://rapids.ai), allowing the computation to be highly
parallel, taking minutes, rather than days.

## Project layout

``` bash
ahah
├── aggregate_lsoa.py  # aggregate outputs to LSOA level
├── create_index.py  # use aggregates to create index
├── get_nhs.py  # retrieve E&W NHS data
├── os_highways.py  # process OS highways data
├── process_air.py  # process air quality data
├── process_routing.py  # process all POI data
├── routing.py  # main routing class
└── common
    ├── logger.py  # use rich logging
    └── utils.py  # utility functions

```

## Methodology

**1. OS Highways road network**  
- Speed estimates given to each road, based on `formOfway` and
`routeHierarchy`  
- Time-weighted distance calculated using length of edge and speed
estimate  
- Node ID converted to sequential integers and saved with edges as
parquet files

**2. Process Data `ahah/process_data.py`**

> This stage prepares the `nodes`, `postcodes`, and `poi` data for use
> in RAPIDS `cugraph`. Makes use of utility functions to assist with
> data preparation from the raw data sources.

- Clean raw data  
- Find the nearest road node to each postcode and point of interest
  using GPU accelerated K Means Clustering  
- Determine minimum buffer distance to use for each point of interest
  - Distances returned for nearest 10 points of interest to each
    postcode using K Means  
  - For each unique POI the maximum distance to associated postcodes is
    taken and saved as a buffer for this POI  
  - Each POI is assigned the postcodes that fall within their KNN, used
    to determine buffer suitability when converted to a graph  
- All processed data written to respective files

<!-- <div align="center">
<img src="ahah/visualisations/buffer.jpg" width="500">
</div> -->

**3. Routing `ahah/routing.py`**

> The routing stage of this project primarily makes use of the RAPIDS
> `cugraph` library. This stage iterates sequentially over each POI of a
> certain type and finds routes to every postcode within a certain
> buffer.

- Iterate over POI of a certain type  
- Create `cuspatial.Graph()` with subset of road nodes using
  `cuspatial.points_in_spatial_window` with buffer
- Run *single-source shortest path* from POI to each node in the sub
  graph
  - `cugraph.sssp` takes into account `weights`, which in this case are
    the `time-weighted` distance of each connection between nodes as
    reported by OSM.  
- `SSSP` distances subset to return only nodes associated with
  postcodes, these distances are added iteratively to a complete
  dataframe of postcodes of which the smallest value for each postcode
  is taken

## AHAH Data Sources

> See the [AHAH V2 FigShare
> Repository](https://figshare.com/articles/online_resource/Access_to_Healthy_Assets_and_Hazards_AHAH_-_Updated_version_2017/8295842/1)
> for the previous iteration.

- [OS
  Highways](https://www.ordnancesurvey.co.uk/business-government/products/mastermap-highways)
- [Air quality](https://uk-air.defra.gov.uk/data/pcm-data)
- [Greenspace](https://osdatahub.os.uk/downloads/open/OpenGreenspace)
- [LSOA
  Polygons](https://borders.ukdataservice.ac.uk/easy_download_data.html?data=England_lsoa_2011)
- [NHS
  England](https://digital.nhs.uk/services/organisation-data-service/data-downloads)
- [NHS Scotland](https://www.opendata.nhs.scot/dataset/)
- [Postcodes](https://geoportal.statistics.gov.uk/search?collection=Dataset&sort=name&tags=all(PRD_ONSPD%2CFEB_2021))
- NDVI (Private)
