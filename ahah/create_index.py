import numpy as np
import pandas as pd
from scipy.stats import norm

from ahah.common.utils import Config, combine_lsoa


def exp_trans(x, df):
    return -23 * np.log(1 - (x / len(df)) * (1 - np.exp(-100 / 23)))


def exp_default(x, df):
    return norm.ppf((x - 0.5) / len(df))


def read_v3():
    v3 = pd.read_csv("./data/out/median_dists.csv")
    v3_secure = pd.read_csv("./data/out/010422_CILLIANBERRAGAN_AHAH_MEDIAN_LSOA.csv")
    air = pd.read_csv("./data/out/lsoa_air.csv")
    return v3.merge(v3_secure, on="lsoa11", how="outer").merge(
        air, on="lsoa11", how="outer"
    )


def read_v2():
    return pd.read_csv("./data/raw/v2/allvariableslsoawdeciles.csv")


def process(idx, low_dist, env_dist, air_qual, high_dist):
    low_dist_ranked = [f"{asset}_ranked" for asset in low_dist]
    env_dist_ranked = [f"{asset}_ranked" for asset in env_dist]
    air_qual_ranked = [f"{asset}_ranked" for asset in air_qual]
    high_dist_ranked = [f"{asset}_ranked" for asset in high_dist]

    low_dist_expd = [f"{asset}_expd" for asset in low_dist]
    env_dist_expd = [f"{asset}_expd" for asset in env_dist]
    air_qual_expd = [f"{asset}_expd" for asset in air_qual]
    high_dist_expd = [f"{asset}_expd" for asset in high_dist]

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

    idx["h_domain"] = idx[low_dist_expd].mean(axis=1)
    idx["g_domain"] = idx[env_dist_expd].mean(axis=1)
    idx["e_domain"] = idx[air_qual_expd].mean(axis=1)
    idx["r_domain"] = idx[high_dist_expd].mean(axis=1)

    idx["r_rank"] = idx["r_domain"].rank(method="min").astype(int)
    idx["h_rank"] = idx["h_domain"].rank(method="min").astype(int)
    idx["g_rank"] = idx["g_domain"].rank(method="min").astype(int)
    idx["e_rank"] = idx["e_domain"].rank(method="min").astype(int)

    idx["r_exp"] = exp_trans(idx["r_rank"], idx)
    idx["h_exp"] = exp_trans(idx["h_rank"], idx)
    idx["g_exp"] = exp_trans(idx["g_rank"], idx)
    idx["e_exp"] = exp_trans(idx["e_rank"], idx)

    idx["ahah"] = idx[["r_exp", "h_exp", "g_exp", "e_exp"]].mean(axis=1)
    idx["r_ahah"] = idx["ahah"].rank(method="min").astype(int)
    idx["d_ahah"] = pd.qcut(idx["r_ahah"], 10, labels=False)
    return idx


if __name__ == "__main__":
    low_dist = ["gpp", "dentists", "pharmacies", "hospitals", "leisure"]
    env_dist = ["gspassive", "bluespace"]
    air_qual = ["no22019", "so22019", "pm102019g"]
    high_dist = ["gambling", "offlicences", "pubs", "tobacconists", "fastfood"]
    v3 = read_v3()
    v3 = process(v3, low_dist, env_dist, air_qual, high_dist)

    low_dist = ["gpp_dist", "ed_dist", "dent_dist", "pharm_dist", "leis_dist"]
    env_dist = ["green_pas", "blue_dist"]
    air_qual = ["no2_mean", "pm10_mean", "so2_mean"]
    high_dist = ["gamb_dist", "ffood_dist", "pubs_dist", "off_dist", "tobac_dist"]
    v2 = read_v2()
    v2 = process(v2, low_dist, env_dist, air_qual, high_dist)

    lsoa = combine_lsoa(
        eng=Config.RAW_DATA / "lsoa" / "england_lsoa_2011.shp",
        scot=Config.RAW_DATA / "lsoa" / "SG_DataZone_Bdry_2011.shp",
        wales=Config.RAW_DATA / "lsoa" / "lsoa_wales_2011.gpkg",
    )

    v3 = lsoa.merge(v3, on="lsoa11", how="outer")
    v2 = lsoa.merge(v2, on="lsoa11", how="outer")

    v3.to_file(Config.OUT_DATA / "v3_lsoa.gpkg", driver="GPKG")
    v2.to_file(Config.OUT_DATA / "v2_lsoa.gpkg", driver="GPKG")
