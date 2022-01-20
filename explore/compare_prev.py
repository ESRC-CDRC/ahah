import geopandas as gpd
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import pandas as pd
from ahah.common.utils import Config
from sklearn.preprocessing import MinMaxScaler

v2 = pd.read_csv(Config.RAW_DATA / "v2" / "allvariableslsoawdeciles.csv")
v3 = pd.read_csv(Config.OUT_DATA / "mean_dists.csv")
lsoa = gpd.read_file(Config.RAW_DATA / "lsoa" / "england_lsoa_2011.shp")

# fig, ax = plt.subplots()
# v2.plot(x="lsoa11", y="gpp_dist", ax=ax, kind="scatter")
# v3.plot(x="lsoa11", y="gpp", ax=ax, kind="scatter")
# plt.show()


test = v2.filter(["lsoa11", "gpp_dist"]).merge(v3[["lsoa11", "gpp"]], on="lsoa11")

test[["gpp", "gpp_dist"]].rank()

test["diff"] = test["gpp"] - test["gpp_dist"]
test["diff"].mean()

# test.plot(x="gpp", y="gpp_dist", kind="scatter")
# plt.show()

test = lsoa.merge(test, left_on="code", right_on="lsoa11")
# test.plot(column="gpp")
# test.plot(column="gpp_dist")


# test["diff_sc"] = MinMaxScaler(feature_range=(-1, 1)).fit_transform(test[["diff"]])
# test.plot(column="diff_sc", legend=True, cmap="seismic")
test.gpp.max()
test.plot(
    column="diff",
    legend=True,
    cmap="seismic",
    norm=colors.TwoSlopeNorm(
        vcenter=0, vmin=test["diff"].min(), vmax=test["diff"].max()
    ),
)
plt.show()

test.plot(
    column="diff",
    legend=True,
    cmap="seismic",
    norm=colors.TwoSlopeNorm(vcenter=0, vmin=-10, vmax=10),
)
plt.show()


test[["lsoa11", "gpp", "gpp_dist"]].sort_values(by=["gpp", "gpp_dist"], ascending=False)
