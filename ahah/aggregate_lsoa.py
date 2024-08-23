import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ahah.common.utils import Paths


def read_dists(dist_files: list[Path], pcs, ndvi) -> pd.DataFrame:
    dfs = [
        pd.read_parquet(file)
        .rename(columns={"time_weighted": re.split(r"_|\.", file.name)[0]})
        .drop(columns=["easting", "northing", "node_id"])
        .reset_index(drop=True)
        for file in dist_files
    ]

    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on="postcode", how="outer")

    merged_df["postcode"] = merged_df["postcode"].str.replace(" ", "")
    merged_df = merged_df.merge(pcs, on="postcode", how="outer")
    merged_df = merged_df.merge(ndvi, on="postcode", how="outer")
    return merged_df.drop(columns=["postcode"]).groupby("LSOA21CD").median()


if __name__ == "__main__":
    dist_files = list(Path(Paths.OUT).glob("*_distances.parquet"))

    pcs = pd.read_parquet(Paths.PROCESSED / "onspd" / "all_postcodes.parquet")
    pcs = gpd.GeoDataFrame(
        pcs,
        geometry=gpd.points_from_xy(pcs["easting"], pcs["northing"]),
        crs="EPSG:27700",
    )

    lsoa = gpd.read_file("./data/raw/gov/LSOA2021/LSOA_2021_EW_BFC_V8.shp")[
        ["LSOA21CD", "geometry"]
    ]
    sgiz = gpd.read_file("./data/raw/gov/SG_DataZone/SG_DataZone_Bdry_2011.shp")[
        ["DataZone", "geometry"]
    ].rename(columns={"DataZone": "LSOA21CD"})
    lsoa = pd.concat([lsoa, sgiz])

    pcs = gpd.sjoin(pcs, lsoa)
    pcs = pcs[["postcode", "LSOA21CD"]]

    ndvi = pd.read_csv(
        Paths.RAW / "ndvi" / "spatia_orbit_postcode_V1_210422.csv",
        usecols=["PCDS", "NDVI_MEDIAN"],  # type: ignore
    ).rename(columns={"PCDS": "postcode", "NDVI_MEDIAN": "gpas"})
    ndvi["postcode"] = ndvi["postcode"].str.replace(" ", "")

    dists = read_dists(dist_files, pcs, ndvi)
    air = pd.read_csv(Paths.OUT / "air" / "AIR-LSOA21CD.csv")
    dists = dists.merge(air, on="LSOA21CD", how="left")
    dists.to_csv(Paths.OUT / "ahah" / "AHAH-V4-LSOA21CD.csv", index=False)
