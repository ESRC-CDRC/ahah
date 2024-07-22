import numpy as np
import pandas as pd
from scipy.stats import norm


def exp_trans(x, df):
    return -23 * np.log(1 - (x / len(df)) * (1 - np.exp(-100 / 23)))


def exp_default(x, df):
    return norm.ppf((x - 0.5) / len(df))


# def read_v3():
#     v3 = pd.read_csv("./data/out/median_dists.csv")
#     v3_secure = pd.read_csv("./data/out/010422_CILLIANBERRAGAN_AHAH_MEDIAN_LSOA.csv")
#     air = pd.read_csv("./data/out/lsoa_air.csv")
#     return v3.merge(v3_secure, on="lsoa11", how="outer").merge(
#         air, on="lsoa11", how="outer"
#     )


def read_v2():
    return pd.read_csv("./data/raw/v2/allvariableslsoawdeciles.csv")


def process(idx, ahv: str):
    low_dist = ["gp", "dent", "phar", "hosp", "leis"]
    env_dist = ["gpas", "blue"]
    air_qual = ["no2", "so2", "pm10"]
    high_dist = ["gamb", "off", "pubs", "tob", "ffood"]

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
    v3 = read_v3()
    v3 = v3.rename(
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
            "offlicences": "off",
            "tobacconists": "tob",
        }
    )
    v3 = v3.drop(
        [
            "easting_x",
            "northing_x",
            "node_id_x",
            "easting_y",
            "northing_y",
            "node_id_y",
            "greenspace",
        ],
        axis=1,
    )
    v3 = process(v3, ahv="ah3")
    v3 = v3[[c for c in v3.columns if not c.endswith("expd")]]

    v3.to_csv(Config.OUT_DATA / "AHAH_V3.csv", index=False)
