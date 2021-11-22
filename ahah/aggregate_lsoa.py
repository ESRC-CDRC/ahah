import dask_geopandas
import geopandas as gpd
import pandas as pd
import re
from ahah.common.utils import Config
from pathlib import Path
from typing import Generator


def read_dists(files: Generator, pc: Path) -> pd.DataFrame:
    dists = pd.read_parquet(pc)
    for file in files:
        name = re.split(r"_|\.", file.name)[1]
        dists = dists.merge(
            pd.read_csv(file).rename(columns={"distance": name}),
            on="postcode",
        )
    return dists


def read_lsoa(lsoa_path: Path, dz_path: Path) -> gpd.GeoDataFrame:
    lsoa = gpd.read_file(lsoa_path).filter(["code", "geometry"], axis=1)
    dz = (
        gpd.read_file(dz_path)
        .filter(["DataZone", "geometry"], axis=1)
        .rename(columns={"DataZone": "code"})
    )
    return lsoa.append(dz)


def aggregate_dists(dists: pd.DataFrame, zones: gpd.GeoDataFrame) -> pd.DataFrame:
    gdf = gpd.GeoDataFrame(
        dists, geometry=gpd.points_from_xy(dists["easting"], dists["northing"])
    ).drop(["easting", "northing"], axis=1)
    gdf.crs = zones.crs
    gddf = dask_geopandas.from_geopandas(gdf, npartitions=16)
    zones_dask = dask_geopandas.from_geopandas(zones, npartitions=16)
    return (
        dask_geopandas.sjoin(zones_dask, gddf)
        .drop("index_right", axis=1)
        .compute()
        .groupby("code")
        .mean()
        .merge(zones, on="code")
    )


if __name__ == "__main__":
    files = Path(Config.OUT_DATA).glob("distances_*.csv")
    pc_path = Path(Config.PROCESSED_DATA / "postcodes.parquet")
    lsoa_path = Path(Config.RAW_DATA / "lsoa" / "england_lsoa_2011.shp")
    dz_path = Path(Config.RAW_DATA / "lsoa" / "SG_DataZone_Bdry_2011.shp")

    dists = read_dists(files=files, pc=pc_path)
    zones = read_lsoa(lsoa_path=lsoa_path, dz_path=dz_path)
    joined = aggregate_dists(dists=dists, zones=zones)
