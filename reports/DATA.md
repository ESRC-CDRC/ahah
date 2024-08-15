# OS Open Roads:

https://api.os.uk/downloads/v1/products/OpenRoads/downloads?area=GB&format=GeoPackage&redirect

https://osdatahub.os.uk/downloads/open/OpenRoads 

# Air

https://uk-air.defra.gov.uk/datastore/pcm/mappm102022g.csv

https://uk-air.defra.gov.uk/datastore/pcm/mapso22022.csv

https://uk-air.defra.gov.uk/datastore/pcm/mapno22022.csv

# Hospitals

## England

https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/other-nhs-organisations

https://files.digital.nhs.uk/assets/ods/current/ets.zip

## Scotland

https://www.opendata.nhs.scot/dataset/hospital-codes

https://www.opendata.nhs.scot/dataset/cbd1802e-0e04-4282-88eb-d7bdcfb120f0/resource/c698f450-eeed-41a0-88f7-c1e40a568acc/download/hospitals.csv

# GP Practices

## England

https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data

https://files.digital.nhs.uk/assets/ods/current/epraccur.zip

## Scotland

https://www.opendata.nhs.scot/dataset/gp-practice-contact-details-and-list-sizes

https://www.opendata.nhs.scot/dataset/f23655c3-6e23-4103-a511-a80d998adb90/resource/54a6e1e3-98a3-4e78-be0d-1e6d6ebdde1d/download/practice_contactdetails_jan2024-open-data.csv

# Dentist

## England

https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/miscellaneous

https://files.digital.nhs.uk/assets/ods/current/egdpprac.zip

## Scotland

https://www.opendata.nhs.scot/dataset/dental-practices-and-patient-registrations

https://www.opendata.nhs.scot/dataset/2f218ba7-6695-4b22-867d-41383ae36de7/resource/bd93cf4f-b1dc-4f50-aa7e-6a543fe957f6/download/nhs-dental-practices-and-nhs-dental-registrations-as-at-30-sep-2023.csv

# Pharmacies

## England

https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data

https://files.digital.nhs.uk/assets/ods/current/edispensary.zip

## Scotland

https://www.opendata.nhs.scot/dataset/dispenser-location-contact-details

https://www.opendata.nhs.scot/dataset/a30fde16-1226-49b3-b13d-eb90e39c2058/resource/1fc6555a-0ce4-46ba-aad6-53ea523a1fed/download/dispenser_contactdetails_nov2023.csv

## Wales

https://nwssp.nhs.wales/ourservices/primary-care-services/general-information/data-and-publications/pharmacy-practice-dispensing-data/

https://nwssp.nhs.wales/ourservices/primary-care-services/primary-care-services-documents/pharmacy-practice-dispensing-data-docs/dispensing-data-report-november-2023

# Bluespace

https://download.geofabrik.de/europe/great-britain.html

This data was then processed to keep only bluespace;

```bash
#!/bin/bash

osmium tags-filter great-britain-latest.osm.pbf nwr/natural=water w/waterway=* -o gb-water.osm.pbf
osmium export gb-water.osm.pbf -o gb-water.geojson
ogr2ogr -f Parquet gb-water.parquet gb-water.geojson 

osmium tags-filter great-britain-latest.osm.pbf w/natural=coastline -o gb-coast.osm.pbf
osmium export gb-coast.osm.pbf -o gb-coast.geojson
ogr2ogr -f Parquet gb-coast.parquet gb-coast.geojson 
```
