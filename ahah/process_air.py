import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Polygon

from ahah.common.logger import logger
from ahah.common.utils import Config, clean_air, combine_lsoa

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
    return gpd.sjoin(grid_gdf, lsoa).groupby("lsoa11")[col].mean()


if __name__ == "__main__":
    logger.info("Starting air quality processing...")

    lsoa = combine_lsoa(
        eng=Config.RAW_DATA / "lsoa" / "england_lsoa_2011.shp",
        scot=Config.RAW_DATA / "lsoa" / "SG_DataZone_Bdry_2011.shp",
        wales=Config.RAW_DATA / "lsoa" / "lsoa_wales_2011.gpkg",
    )
    no = clean_air(path=Config.RAW_DATA / "air/mapno22019.csv", col="no22019")
    so = clean_air(path=Config.RAW_DATA / "air/mapso22019.csv", col="so22019")
    pm = clean_air(path=Config.RAW_DATA / "air/mappm102019g.csv", col="pm102019g")
    no = interpolate_air(air=no, col="no22019", lsoa=lsoa, grid_size=GRID_SIZE)
    so = interpolate_air(air=so, col="so22019", lsoa=lsoa, grid_size=GRID_SIZE)
    pm = interpolate_air(air=pm, col="pm102019g", lsoa=lsoa, grid_size=GRID_SIZE)

    logger.debug(f"Saving air dataframe to {Config.OUT_DATA / 'lsoa_air.csv'}")
    air_dfs = [pd.DataFrame(df) for df in [no, so, pm]]

    lsoa_air = lsoa.set_index("lsoa11").join(air_dfs).reset_index()
    lsoa_air[["lsoa11", "no22019", "so22019", "pm102019g"]].to_csv(
        Config.OUT_DATA / "lsoa_air.csv", index=False
    )
