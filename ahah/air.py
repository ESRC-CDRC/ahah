import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Polygon

from ahah.common.logger import logger
from ahah.common.utils import Config, Paths, clean_air

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
    air: pd.DataFrame, col: str, msoa: gpd.GeoDataFrame, grid_size: int
):
    logger.debug(f"Interpolating air: {col}")
    grid_x, grid_y = np.mgrid[  # type:ignore
        0 : max(air.x) : grid_size, 0 : max(air.y) : grid_size
    ]
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
    return gpd.sjoin(grid_gdf, msoa).groupby("MSOA21CD")[col].mean()


if __name__ == "__main__":
    logger.info("Starting air quality processing...")

    msoa = gpd.read_file(Paths.RAW / "gov" / "msoa-2021-bfc.gpkg")
    no = clean_air(path=Paths.RAW / "air/mapno22022.csv", col="no22022")
    so = clean_air(path=Paths.RAW / "air/mapso22022.csv", col="so22022")
    pm = clean_air(path=Paths.RAW / "air/mappm102022g.csv", col="pm102022g")
    no = interpolate_air(air=no, col="no22022", msoa=msoa, grid_size=GRID_SIZE)
    so = interpolate_air(air=so, col="so22022", msoa=msoa, grid_size=GRID_SIZE)
    pm = interpolate_air(air=pm, col="pm102022g", msoa=msoa, grid_size=GRID_SIZE)

    logger.debug(f"Saving air dataframe to {Paths.OUT / 'msoa-2021-air.csv'}")
    air_dfs = [pd.DataFrame(df) for df in [no, so, pm]]

    msoa_air = msoa.set_index("MSOA21CD").join(air_dfs).reset_index()
    msoa_air[["MSOA21CD", "no22022", "so22022", "pm102022g"]].to_csv(
        Paths.OUT / "air" / "msoa-2021-air.csv", index=False
    )
