# AHAH Version 4 Data Overview

This document provides a comprehensive overview of the datasets used in the AHAH Version 4 data product. Each dataset is described in terms of its content, source, and production date. The data has been carefully selected and processed to ensure accuracy and relevance for our analyses.

All data used to produce AHAH Version 4 was the most recent available at the time of production.

# OS Open Roads

- **Description**: This dataset provides detailed information about the road network in Great Britain.
- **Source**: [OS Open Roads](https://api.os.uk/downloads/v1/products/OpenRoads/downloads?area=GB&format=GeoPackage&redirect)
- **Production Date**: 2024

# Air Quality Data

- **Description**: Contains air quality data, including pollutant concentrations across the UK.
- **Sources**:
  - [UK Air Data](https://uk-air.defra.gov.uk/data/pcm-data)
  - [PM10 Data](https://uk-air.defra.gov.uk/datastore/pcm/mappm102022g.csv)
  - [SO2 Data](https://uk-air.defra.gov.uk/datastore/pcm/mapso22022.csv)
  - [NO2 Data](https://uk-air.defra.gov.uk/datastore/pcm/mapno22022.csv)
- **Production Date**: 2022

# Hospitals

## England

- **Description**: Lists NHS organisations and their details in England.
- **Source**: [NHS Digital](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/other-nhs-organisations); NHS trust sites
- **Production Date**: 2024

## Scotland

- **Description**: Provides hospital codes and details in Scotland.
- **Source**: [NHS Scotland Open Data](https://www.opendata.nhs.scot/dataset/hospital-codes)
- **Production Date**: 2024

# GP Practices

## England

- **Description**: Contains data on GP practices in England.
- **Source**: [NHS Digital](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data); GP Practices
- **Production Date**: 2024

## Scotland

- **Description**: Provides contact details and list sizes for GP practices in Scotland.
- **Source**: [NHS Scotland Open Data](https://www.opendata.nhs.scot/dataset/gp-practice-contact-details-and-list-sizes)
- **Production Date**: 2024

# Dentists

## England

- **Description**: Data on dental practices in England.
- **Source**: [NHS Digital](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/miscellaneous); General Dental Practices
- **Production Date**: 2024

## Scotland

- **Description**: Information on dental practices and patient registrations in Scotland.
- **Source**: [NHS Scotland Open Data](https://www.opendata.nhs.scot/dataset/dental-practices-and-patient-registrations)
- **Production Date**: 2024

# Pharmacies

## England

- **Description**: Data on dispensaries in England.
- **Source**: [NHS Digital](https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data); Dispensaries
- **Production Date**: 2024

## Scotland

- **Description**: Contact details for dispensers in Scotland.
- **Source**: [NHS Scotland Open Data](https://www.opendata.nhs.scot/dataset/dispenser-location-contact-details)
- **Production Date**: 2024

## Wales

- **Description**: Pharmacy practice dispensing data in Wales.
- **Source**: [NHS Wales](https://nwssp.nhs.wales/ourservices/primary-care-services/general-information/data-and-publications/pharmacy-practice-dispensing-data/)
- **Production Date**: 2023

# Bluespace

- **Description**: Geospatial data of water bodies and coastlines in Great Britain.
- **Source**: [Geofabrik](https://download.geofabrik.de/europe/great-britain.html)
- **Production Date**: 2024

This data was then processed to keep only bluespace:

```bash
#!/bin/bash

osmium tags-filter great-britain-latest.osm.pbf nwr/natural=water w/waterway=* -o gb-water.osm.pbf
osmium export gb-water.osm.pbf -o gb-water.geojson
ogr2ogr -f Parquet gb-water.parquet gb-water.geojson 

osmium tags-filter great-britain-latest.osm.pbf w/natural=coastline -o gb-coast.osm.pbf
osmium export gb-coast.osm.pbf -o gb-coast.geojson
ogr2ogr -f Parquet gb-coast.parquet gb-coast.geojson 
```
