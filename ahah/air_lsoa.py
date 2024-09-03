import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Polygon

from ahah.common.utils import Paths, clean_air

GRID_SIZE = 1000


def create_polygon(x: float, y: float, grid_size: int):
    return Polygon(
        [
            [x, y],
            [x + grid_size, y],
            [x + grid_size, y + grid_size],
            [x, y + grid_size],
            [x, y],
        ]
    )


def interpolate_air(
    air: pd.DataFrame, col: str, lsoa: gpd.GeoDataFrame, grid_size: int
):
    grid_x, grid_y = np.mgrid[0 : max(air.x) : grid_size, 0 : max(air.y) : grid_size]
    grid_z = griddata(
        points=air[["x", "y"]].astype("int").to_numpy(),
        values=air[col].values,
        xi=(grid_x, grid_y),
        method="nearest",
    )

    grid_x = grid_x.flatten()
    grid_y = grid_y.flatten()
    grid_z = grid_z.flatten()

    grid_df = pd.DataFrame({"x": grid_x, "y": grid_y, col: grid_z})

    grid_df["geometry"] = pd.Series(
        [create_polygon(row.x, row.y, grid_size) for row in grid_df.itertuples()]
    )

    grid_gdf = gpd.GeoDataFrame(grid_df, geometry="geometry", crs=27700)
    return gpd.sjoin(grid_gdf, lsoa).groupby("LSOA21CD")[col].mean()


if __name__ == "__main__":
    lsoa = gpd.read_file("./data/raw/gov/LSOA2021/LSOA_2021_EW_BFC_V8.shp")[
        ["LSOA21CD", "geometry"]
    ]
    sgiz = gpd.read_file("./data/raw/gov/SG_DataZone/SG_DataZone_Bdry_2011.shp")[
        ["DataZone", "geometry"]
    ].rename(columns={"DataZone": "LSOA21CD"})
    lsoa = pd.concat([lsoa, sgiz])
    try:
        no = clean_air(path=Paths.RAW / "air/mapno22022.csv", col="no22022")
    except Exception as e:
        raise RuntimeError(f"Error cleaning air data for NO2: {e}")
    so = clean_air(path=Paths.RAW / "air/mapso22022.csv", col="so22022")
    pm = clean_air(path=Paths.RAW / "air/mappm102022g.csv", col="pm102022g")
    try:
        no = interpolate_air(air=no, col="no22022", lsoa=lsoa, grid_size=GRID_SIZE)
    except Exception as e:
        raise RuntimeError(f"Error interpolating air data for NO2: {e}")
    so = interpolate_air(air=so, col="so22022", lsoa=lsoa, grid_size=GRID_SIZE)
    pm = interpolate_air(air=pm, col="pm102022g", lsoa=lsoa, grid_size=GRID_SIZE)

    air_dfs = [pd.DataFrame(df) for df in [no, so, pm]]

    lsoa_air = lsoa.set_index("LSOA21CD").join(air_dfs).reset_index()
    lsoa_air[["LSOA21CD", "no22022", "so22022", "pm102022g"]].to_csv(
        Paths.OUT / "air" / "AIR-LSOA21CD.csv", index=False
    )
