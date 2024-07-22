from pathlib import Path
import h3pandas
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd

breaks = [0, 1, 5, 10, 15, 30, float("inf")]

csv_files = list(Path("./data/out/").glob("*.csv"))


# Determine the number of rows and columns for subplots
n_files = len(csv_files)
n_cols = 3  # You can adjust this as needed
n_rows = (n_files + n_cols - 1) // n_cols

fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
axes = axes.flatten()  # Flatten in case we have more than one row of subplots
vmin = float("inf")
vmax = float("-inf")

for i, file in enumerate(csv_files):
    df = pd.read_csv(file)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.easting, df.northing), crs=27700
    ).to_crs(4326)
    gdf["lon"], gdf["lat"] = gdf.geometry.x, gdf.geometry.y

    h3_df = gdf[["lon", "lat", "distance"]].h3.geo_to_h3_aggregate(
        5, lat_col="lat", lng_col="lon", operation="mean"
    )
    h3_df["decile"] = pd.qcut(h3_df["distance"], 10, labels=False)

    ax = axes[i]
    plot = h3_df.plot(column="decile", cmap="viridis", ax=ax, legend=False)
    ax.set_title(file.stem)
    ax.axis("off")

    vmin = min(vmin, h3_df["decile"].min())
    vmax = max(vmax, h3_df["decile"].max())

# Hide any unused subplots
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])
# cmap = plt.cm.get_cmap(
#     "viridis", vmax - vmin + 1
# )  # Adjust cmap to have enough colors for each decile
# norm = mcolors.BoundaryNorm(range(vmin, vmax + 1), cmap.N)
# sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
# sm.set_array([])  # fake empty array for the scalar mappable
# cbar = plt.colorbar(sm, ax=axes, orientation="horizontal", pad=0.02, aspect=30)
#
# ticks = range(vmin, vmax + 1)
# cbar.set_ticks(ticks)
# cbar.set_ticklabels([f"Decile {i + 1}" for i in ticks])
# cbar.set_label("Deciles")
plt.tight_layout()
# plt.subplots_adjust(bottom=0.25, hspace=0.1)
plt.show()
