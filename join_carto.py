import geopandas as gpd
import pandas as pd

ahah = pd.read_csv("./data/out/AHAH_V3.csv")
ahah.columns
lsoa = gpd.read_file("./lsoa11_uk_detail_urban_copy.shp")

merged = lsoa.merge(ahah, left_on="lsoa11_cd", right_on="lsoa11", how="left")
merged.to_file("./lsoa11_uk_detail_urban_merged.shp")
