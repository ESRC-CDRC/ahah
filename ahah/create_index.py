import geopandas as gpd
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ahah.common.utils import Config

# FIX: currently uses v2 data to test methods
from scipy.stats import norm

low_dist = [
    "gpp_dist",
    "ed_dist",
    "dent_dist",
    "pharm_dist",
    "leis_dist",
]
env_dist = [
    "blue_dist",
    "green_pas",
    "green_act",
]
high_dist = ["gamb_dist", "ffood_dist", "pubs_dist", "off_dist", "tobac_dist"]
air_qual = ["no2_mean", "pm10_mean", "so2_mean"]

v2 = pd.read_csv("data/raw/v2/allvariableslsoawdeciles.csv")
v2 = v2[[c for c in v2.columns if not c.startswith("d_")]]
v3 = pd.read_csv("data/out/mean_dists.csv")

v2[low_dist] = v2[low_dist].rank(method="min").astype(int)
v2[air_qual] = v2[air_qual].rank(method="min").astype(int)
v2[env_dist] = v2[env_dist].rank(method="min").astype(int)
v2[high_dist] = v2[high_dist].rank(method="min", ascending=False).astype(int)

v3["gpp"] = v3["gpp"].rank(method="min").astype(int)

v2[["lsoa11", "gpp_dist"]]
v3[["lsoa11", "gpp"]].merge(v2[["lsoa11", "gpp_dist"]], on="lsoa11")


def exp_default(x, df):
    return norm.ppf((x - 0.5) / len(df))


v2[low_dist + air_qual + env_dist + high_dist] = exp_default(
    v2[low_dist + air_qual + env_dist + high_dist], v2
)
v3["gpp"] = exp_default(v3["gpp"], v3)
v2["gpp_dist"]

test = v3[["lsoa11", "gpp"]].merge(v2[["lsoa11", "gpp_dist"]], on="lsoa11")
test["diff"] = test["gpp"] - test["gpp_dist"]
test["diff"].mean()

lsoa = gpd.read_file(Config.RAW_DATA / "lsoa" / "england_lsoa_2011.shp")
test = lsoa.merge(test, left_on="code", right_on="lsoa11")

test.plot(
    column="diff",
    legend=True,
    cmap="seismic",
    norm=colors.TwoSlopeNorm(
        vcenter=0, vmin=test["diff"].min(), vmax=test["diff"].max()
    ),
)
plt.show()


v2["r_domain"] = v2[high_dist].mean(axis=1)
v2["h_domain"] = v2[low_dist].mean(axis=1)
v2["g_domain"] = v2[env_dist].mean(axis=1)
v2["e_domain"] = v2[air_qual].mean(axis=1)

v2["r_rank"] = v2["r_domain"].rank(method="min").astype(int)
v2["h_rank"] = v2["h_domain"].rank(method="min").astype(int)
v2["g_rank"] = v2["g_domain"].rank(method="min").astype(int)
v2["e_rank"] = v2["e_domain"].rank(method="min").astype(int)


def exp_trans(x, df):
    return -23 * np.log(1 - (x / len(df)) * (1 - np.exp(-100 / 23)))


v2["r_exp"] = exp_trans(v2["r_rank"], v2)
v2["h_exp"] = exp_trans(v2["h_rank"], v2)
v2["g_exp"] = exp_trans(v2["g_rank"], v2)
v2["e_exp"] = exp_trans(v2["e_rank"], v2)

v2["ahah"] = v2[["r_exp", "h_exp", "g_exp", "e_exp"]].mean(axis=1)
