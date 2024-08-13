import re
from pathlib import Path
from typing import Generator

import cudf
import geopandas as gpd
import pandas as pd

from ahah.common.utils import Paths


def read_dists(dist_files: list[Path], pcs: cudf.DataFrame) -> pd.DataFrame:
    dfs = [
        pd.read_parquet(file)
        .rename(columns={"distance": re.split(r"_|\.", file.name)[1]})
        .drop(columns=["easting", "northing", "index"])
        .reset_index(drop=True)
        for file in dist_files
    ]

    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on="postcode", how="outer")

    return (
        merged_df.merge(pcs, on="postcode", how="left")
        .drop(columns=["postcode"])
        .groupby("MSOA11CD")
        .median()
    )


if __name__ == "__main__":
    dist_files = list(Path(Paths.OUT).glob("distances_*.parquet"))

    pcs = pd.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")
    pcs = gpd.GeoDataFrame(
        pcs,
        geometry=gpd.points_from_xy(pcs["easting"], pcs["northing"]),
        crs="EPSG:27700",
    )
    msoa = gpd.read_file("./data/raw/gov/msoa-2011-bfc.gpkg")[["MSOA11CD", "geometry"]]
    sgiz = gpd.read_file("./data/raw/gov/SG_IntermediateZone_Bdry_2011.shp")[
        ["InterZone", "geometry"]
    ].rename(columns={"InterZone": "MSOA11CD"})
    msoa = pd.concat([msoa, sgiz])
    pcs = gpd.sjoin(pcs, msoa)
    pcs = pcs[["postcode", "MSOA11CD"]]

    dists = read_dists(dist_files, pcs)
    air = pd.read_csv(Paths.OUT / "air" / "AIR-MSOA11CD.csv")
    dists = dists.merge(air, on="MSOA11CD", how="left")
    dists.to_csv(Paths.OUT / "DRIVETIME-MSOA11CD.csv", index=False)
