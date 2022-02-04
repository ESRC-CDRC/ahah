import cudf
import pandas as pd
from ahah.common.utils import Config
from ahah.routing import Routing

if __name__ == "__main__":
    edges = cudf.read_parquet(Config.OS_GRAPH / "edges.parquet")
    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    postcodes = cudf.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet")

    gpp = pd.read_parquet(Config.PROCESSED_DATA / "gpp.parquet")
    OUT_FILE = Config.OUT_DATA / "NO_BUFFER_distances_gpp.csv"

    if not OUT_FILE.exists():
        routing = Routing(
            name="gpp",
            edges=edges,
            nodes=nodes,
            postcodes=postcodes,
            pois=gpp,
            weights="time_weighted",
            buffer=False,
        )
        routing.fit()

        distances = routing.distances.join(postcodes.set_index("node_id")).reset_index()
        distances[["postcode", "distance"]].to_csv(OUT_FILE, index=False)
