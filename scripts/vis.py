import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("./data/out/distances_gpp.csv")
df["distance_rank"] = df["distance"].rank()
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.easting, df.northing))
gdf.plot(column="distance_rank", legend=True)
plt.show()
