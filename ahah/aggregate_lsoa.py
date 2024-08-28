import re
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ahah.common.utils import Paths


def read_dists(
    dist_files: list[Path], pcs: pd.DataFrame, ndvi: pd.DataFrame, ldc: pd.DataFrame
) -> pd.DataFrame:
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
    merged_df = merged_df.drop(columns=["postcode"]).groupby("LSOA21CD").median()
    merged_df["tobacconists"] = sys.maxsize
    merged_df["gambling"] = sys.maxsize

    def compare_and_replace(x, y):
        return y if pd.notna(y) and (pd.isna(x) or y < x) else x

    for column in merged_df.columns:
        if column == "LSOA21CD":
            continue
        if column in ldc.columns:
            merged_df[column] = merged_df[column].combine(
                ldc[column], compare_and_replace
            )
            merged_df[column].combine(ldc[column], compare_and_replace)
    return merged_df


def main():
    dist_files: list[Path] = list(Path(Paths.OUT).glob("*_distances.parquet"))

    pcs: pd.DataFrame = pd.read_parquet(
        Paths.PROCESSED / "onspd" / "all_postcodes.parquet"
    )
    pcs = gpd.GeoDataFrame(
        pcs,
        geometry=gpd.points_from_xy(pcs["easting"], pcs["northing"]),
        crs="EPSG:27700",
    )

    lsoa: gpd.GeoDataFrame = gpd.read_file(
        "./data/raw/gov/LSOA2021/LSOA_2021_EW_BFC_V8.shp"
    )[["LSOA21CD", "geometry"]]
    sgiz: gpd.GeoDataFrame = gpd.read_file(
        "./data/raw/gov/SG_DataZone/SG_DataZone_Bdry_2011.shp"
    )[["DataZone", "geometry"]].rename(columns={"DataZone": "LSOA21CD"})
    lsoa: gpd.GeoDataFrame = pd.concat([lsoa, sgiz])  # type: ignore

    pcs = gpd.sjoin(pcs, lsoa)[["postcode", "LSOA21CD"]]

    ndvi: pd.DataFrame = pd.read_csv(
        Paths.RAW / "ndvi" / "spatia_orbit_postcode_V1_210422.csv",
        usecols=["PCDS", "NDVI_MEDIAN"],  # type: ignore
    ).rename(columns={"PCDS": "postcode", "NDVI_MEDIAN": "gpas"})
    ndvi["postcode"] = ndvi["postcode"].str.replace(" ", "")

    ldc: pd.DataFrame = pd.read_csv(
        Paths.PROCESSED / "2024_08_21_CILLIANBERRAGAN_AHAHV4_LDC.csv"
    ).set_index("LSOA21CD")
    dists: pd.DataFrame = read_dists(dist_files, pcs, ndvi, ldc)
    air: pd.DataFrame = pd.read_csv(Paths.OUT / "air" / "AIR-LSOA21CD.csv")
    dists = dists.merge(air, on="LSOA21CD", how="left")
    dists.to_csv(Paths.OUT / "ahah" / "AHAH-V4-LSOA21CD.csv", index=False)


if __name__ == "__main__":
    main()
