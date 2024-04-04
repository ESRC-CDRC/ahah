import re
from pathlib import Path
from typing import Generator

import cudf
import pandas as pd

from ahah.common.utils import Config, fix_postcodes


def read_dists(dist_files: Generator, pcs: cudf.DataFrame, ndvi) -> pd.DataFrame:
    dfs = (
        [
            cudf.read_csv(file)
            .drop_duplicates("postcode")
            .set_index("postcode")
            .rename(columns={"distance": re.split(r"_|\.", file.name)[1]})
            for file in dist_files
        ],
    )
    dfs = cudf.concat(dfs, axis=1).reset_index().pipe(fix_postcodes)

    return (
        dfs.set_index("postcode")
        .join(ndvi)
        .join(pcs)
        .reset_index()
        .groupby("lsoa11")
        .median()
    )


if __name__ == "__main__":
    dist_files = list(Path(Config.OUT_DATA).glob("distances_*.csv"))

    pcs = pd.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet").set_index(
        "postcode"
    )
    gpp = pd.read_csv(Path(Config.OUT_DATA / "distances_gpp.csv"))
    gpp = pcs.merge(gpp, on="postcode")

    gspassive = (
        cudf.read_csv(Config.RAW_DATA / "ndvi" / "sentinel_postcode_ndvi_20210419.csv")
        .rename(columns={"PCDS": "postcode", "NDVI_MEDIAN": "gspassive"})[
            ["postcode", "gspassive"]
        ]
        .set_index("postcode")
    )
    pcs[pcs["lsoa11"] == "E01019077"]
    dists = read_dists(dist_files, pcs, gspassive)
    dists.to_csv(Config.OUT_DATA / "median_dists.csv")
