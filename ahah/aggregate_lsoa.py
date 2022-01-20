import cudf
import pandas as pd
import re
from ahah.common.utils import Config
from pathlib import Path
from typing import Generator


def read_dists(dist_files: Generator, pc: cudf.DataFrame, uprn) -> pd.DataFrame:
    dfs = cudf.concat(
        [
            cudf.read_csv(file)
            .set_index("postcode")
            .rename(columns={"distance": re.split(r"_|\.", file.name)[1]})
            for file in dist_files
        ],
        axis=1,
    )

    dfs = dfs.merge(uprn, on="postcode").dropna()
    dfs["num_pc"] = 1
    for poi in Config.POI_LIST:
        dfs[poi] = dfs[poi] * dfs["uprn_count"]

    dfs = dfs.groupby("lsoa11").sum()

    for poi in Config.POI_LIST:
        dfs[poi] = dfs[poi] / dfs["uprn_count"]

    return dfs
    # return (
    #     pc.set_index("postcode")
    #     .join(dfs, how="left")
    #     .dropna()
    #     .groupby("lsoa11")
    #     .median()
    # )


if __name__ == "__main__":
    dist_files = list(Path(Config.OUT_DATA).glob("distances_*.csv"))
    uprn = (
        cudf.read_parquet(
            Config.OUT_DATA / "uprn_pcs.parquet",
        )
        .rename(columns={"lsoa11cd": "lsoa11"})
        .drop(["oa11cd"], axis=1)
    )

    pc = cudf.read_csv(
        Config.RAW_DATA / "onspd" / "postcodes.csv",
        usecols=["pcds", "lsoa11"],
    ).rename(columns={"pcds": "postcode"})

    dists = read_dists(dist_files, pc, uprn)
    dists.to_csv(Config.OUT_DATA / "median_dists.csv")
