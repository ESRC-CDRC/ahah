import numpy as np
import pandas as pd
from scipy.stats import norm

from ahah.common.utils import Paths


def exp_trans(x, df):
    return -23 * np.log(1 - (x / len(df)) * (1 - np.exp(-100 / 23)))


def exp_default(x, df):
    return norm.ppf((x - 0.5) / len(df))


def process(idx, ahv: str):
    low_dist = ["gp", "dent", "phar", "hosp", "leis"]
    env_dist = ["gpas", "blue"]
    air_qual = ["no2", "so2", "pm10"]
    high_dist = ["gamb", "pubs", "tob", "ffood"]

    all_dists = low_dist + env_dist + air_qual + high_dist
    idx = idx.rename(columns={col: f"{ahv}{col}" for col in all_dists})

    low_dist = [f"{ahv}{asset}" for asset in low_dist]
    env_dist = [f"{ahv}{asset}" for asset in env_dist]
    air_qual = [f"{ahv}{asset}" for asset in air_qual]
    high_dist = [f"{ahv}{asset}" for asset in high_dist]

    low_dist_ranked = [f"{asset}_rnk" for asset in low_dist]
    env_dist_ranked = [f"{asset}_rnk" for asset in env_dist]
    air_qual_ranked = [f"{asset}_rnk" for asset in air_qual]
    high_dist_ranked = [f"{asset}_rnk" for asset in high_dist]

    low_dist_expd = [f"{asset}_expd" for asset in low_dist]
    env_dist_expd = [f"{asset}_expd" for asset in env_dist]
    air_qual_expd = [f"{asset}_expd" for asset in air_qual]
    high_dist_expd = [f"{asset}_expd" for asset in high_dist]

    low_dist_pct = [f"{asset}_pct" for asset in low_dist]
    env_dist_pct = [f"{asset}_pct" for asset in env_dist]
    air_qual_pct = [f"{asset}_pct" for asset in air_qual]
    high_dist_pct = [f"{asset}_pct" for asset in high_dist]

    idx[low_dist_ranked] = idx[low_dist].rank(method="min").astype(int)
    idx[env_dist_ranked] = idx[env_dist].rank(method="min").astype(int)
    idx[air_qual_ranked] = idx[air_qual].rank(method="min").astype(int)
    idx[high_dist_ranked] = (
        idx[high_dist].rank(method="min", ascending=False).astype(int)
    )

    # higher values os gspassive are better (prop of pc that is gs for v3)
    # (number of near gs in v2)
    idx[env_dist_ranked[0]] = (
        idx[env_dist[0]].rank(method="min", ascending=False).astype(int)
    )

    idx[low_dist_expd] = exp_default(idx[low_dist_ranked], idx)
    idx[env_dist_expd] = exp_default(idx[env_dist_ranked], idx)
    idx[air_qual_expd] = exp_default(idx[air_qual_ranked], idx)
    idx[high_dist_expd] = exp_default(idx[high_dist_ranked], idx)

    idx[low_dist_pct] = idx[low_dist_ranked].apply(
        lambda x: (x / x.max() * 100).astype(int)
    )
    idx[env_dist_pct] = idx[env_dist_ranked].apply(
        lambda x: (x / x.max() * 100).astype(int)
    )
    idx[air_qual_pct] = idx[air_qual_ranked].apply(
        lambda x: (x / x.max() * 100).astype(int)
    )
    idx[high_dist_pct] = idx[high_dist_ranked].apply(
        lambda x: (x / x.max() * 100).astype(int)
    )

    idx[f"{ahv}h"] = idx[low_dist_expd].mean(axis=1)
    idx[f"{ahv}g"] = idx[env_dist_expd].mean(axis=1)
    idx[f"{ahv}e"] = idx[air_qual_expd].mean(axis=1)
    idx[f"{ahv}r"] = idx[high_dist_expd].mean(axis=1)

    idx[f"{ahv}h_rnk"] = idx[f"{ahv}h"].rank(method="min").astype(int)
    idx[f"{ahv}g_rnk"] = idx[f"{ahv}g"].rank(method="min").astype(int)
    idx[f"{ahv}e_rnk"] = idx[f"{ahv}e"].rank(method="min").astype(int)
    idx[f"{ahv}r_rnk"] = idx[f"{ahv}r"].rank(method="min").astype(int)

    idx[f"{ahv}h_pct"] = pd.qcut(idx[f"{ahv}h_rnk"], 100, labels=False) + 1
    idx[f"{ahv}g_pct"] = pd.qcut(idx[f"{ahv}g_rnk"], 100, labels=False) + 1
    idx[f"{ahv}e_pct"] = pd.qcut(idx[f"{ahv}e_rnk"], 100, labels=False) + 1
    idx[f"{ahv}r_pct"] = pd.qcut(idx[f"{ahv}r_rnk"], 100, labels=False) + 1

    idx["h_expd"] = exp_trans(idx[f"{ahv}h_rnk"], idx)
    idx["g_expd"] = exp_trans(idx[f"{ahv}g_rnk"], idx)
    idx["e_expd"] = exp_trans(idx[f"{ahv}e_rnk"], idx)
    idx["r_expd"] = exp_trans(idx[f"{ahv}r_rnk"], idx)

    idx[f"{ahv}ahah"] = idx[["r_expd", "h_expd", "g_expd", "e_expd"]].mean(axis=1)
    idx[f"{ahv}ahah_rn"] = idx[f"{ahv}ahah"].rank(method="min").astype(int)
    idx[f"{ahv}ahah_pc"] = pd.qcut(idx[f"{ahv}ahah_rn"], 100, labels=False) + 1
    return idx


if __name__ == "__main__":
    v4 = pd.read_csv("./data/out/ahah/AHAH-V4-LSOA21CD.csv")
    ldc = pd.read_csv("./data/processed/2024_08_21_CILLIANBERRAGAN_AHAHV4_LDC.csv")
    v4 = v4.merge(ldc, on="LSOA21CD")
    v4 = v4.rename(
        columns={
            "gpp": "gp",
            "dentists": "dent",
            "pharmacies": "phar",
            "hospitals": "hosp",
            "leisure": "leis",
            "gspassive": "gpas",
            "bluespace": "blue",
            "no22019": "no2",
            "so22019": "so2",
            "pm102019g": "pm10",
            "fastfood": "ffood",
            "gambling": "gamb",
            "tobacconists": "tob",
            "no22022": "no2",
            "so22022": "so2",
            "pm102022g": "pm10",
        }
    ).ffill()
    v4 = process(v4, ahv="ah4")
    v4 = v4[[c for c in v4.columns if not c.endswith("expd")]]

    v4.to_csv(Paths.OUT / "ahah" / "AHAH_V4.csv", index=False)
