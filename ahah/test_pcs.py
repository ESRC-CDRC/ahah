import matplotlib.pyplot as plt
import pandas as pd

from ahah.common.utils import Config

pcs = pd.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet").set_index("postcode")
gpp = pd.read_csv(Config.OUT_DATA / "distances_gpp.csv")
gpp = pcs.merge(gpp, on="postcode")

gpp.plot.scatter(x="easting", y="northing", c="distance")
plt.savefig("tmp.png")
