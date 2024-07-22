import re
from pathlib import Path
from typing import Generator

import cudf
import pandas as pd

from ahah.common.utils import Config, Paths


def read_dists(dist_files: Generator, pcs: cudf.DataFrame) -> pd.DataFrame:
    dfs = [
        cudf.from_pandas(pd.read_csv(file))
        .drop_duplicates("postcode")
        .set_index("postcode")
        .rename(columns={"distance": re.split(r"_|\.", file.name)[1]})
        for file in dist_files
    ]
    dfs = cudf.concat(dfs).reset_index()

    return (
        dfs.set_index("postcode")
        # .join(ndvi)
        .join(pcs)
        .reset_index()
        .groupby("MSOA21CD")
        .median()
    )


if __name__ == "__main__":
    dist_files = list(Path(Paths.OUT).glob("distances_*.csv"))

    pcs = pd.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet").set_index(
        "postcode"
    )

    # gspassive = (
    #     cudf.read_csv(Config.RAW / "ndvi" / "sentinel_postcode_ndvi_20210419.csv")
    #     .rename(columns={"PCDS": "postcode", "NDVI_MEDIAN": "gspassive"})[
    #         ["postcode", "gspassive"]
    #     ]
    #     .set_index("postcode")
    # )
    dists = read_dists(dist_files, pcs)
    dists.to_csv(Config.OUT / "median_dists-v4.csv")
