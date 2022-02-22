import re
from pathlib import Path
from typing import Generator

import cudf
import pandas as pd

from ahah.common.utils import Config, fix_postcodes


def read_dists(dist_files: Generator, uprn: cudf.DataFrame, ndvi) -> pd.DataFrame:
    dfs = (
        cudf.concat(
            [
                cudf.read_csv(file)
                .set_index("postcode")
                .rename(columns={"distance": re.split(r"_|\.", file.name)[1]})
                for file in dist_files
            ],
            axis=1,
        )
        .reset_index()
        .pipe(fix_postcodes)
    )
    dfs = dfs.set_index("postcode").join(ndvi).join(uprn)

    for poi in Config.POI_LIST + ["gspassive"]:
        dfs[poi] = dfs[poi] * dfs["uprn_count"]

    dfs = dfs.groupby("lsoa11").sum()

    for poi in Config.POI_LIST + ["gspassive"]:
        dfs[poi] = dfs[poi] / dfs["uprn_count"]
    return dfs.drop("uprn_count", axis=1)


if __name__ == "__main__":
    dist_files = list(Path(Config.OUT_DATA).glob("distances_*.csv"))

    uprn = (
        cudf.read_parquet(
            Config.PROCESSED_DATA / "uprn_pcs.parquet",
        )
        .rename(columns={"lsoa11cd": "lsoa11"})
        .drop(["oa11cd"], axis=1)
        .pipe(fix_postcodes)
    )
    uprn = uprn[~uprn["lsoa11"].str.contains(r"^\d.*")].set_index("postcode")

    gspassive = (
        cudf.read_csv(Config.RAW_DATA / "ndvi" / "sentinel_postcode_ndvi_20210419.csv")
        .rename(columns={"PCDS": "postcode", "NDVI_MEDIAN": "gspassive"})[
            ["postcode", "gspassive"]
        ]
        .set_index("postcode")
    )
    dists = read_dists(dist_files, uprn, gspassive)
    dists.to_csv(Config.OUT_DATA / "weighted_mean_dists.csv")
