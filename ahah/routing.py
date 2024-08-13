import cudf
import pandas as pd
from tqdm import tqdm
from ukroutes import Routing
from ukroutes.common.utils import Paths
from ukroutes.process_routing import add_to_graph, add_topk


def _routing(df, name, outputs, nodes, edges):
    inputs = df.to_pandas().dropna()
    outputs = outputs.to_pandas().dropna()

    nodes_c = nodes.copy()
    edges_c = edges.copy()

    inputs, nodes_c, edges_c = add_to_graph(inputs, nodes_c, edges_c, 1)
    outputs, nodes_c, edges_c = add_to_graph(outputs, nodes_c, edges_c, 1)

    inputs = add_topk(inputs, outputs, 10)

    routing = Routing(
        edges=edges_c,
        nodes=nodes_c,
        outputs=outputs,
        inputs=inputs,
        weights="time_weighted",
        min_buffer=5000,
        max_buffer=500_000,
    )
    routing.fit()
    distances = (
        routing.distances.set_index("vertex")
        .join(cudf.from_pandas(outputs).set_index("node_id"), how="right")
        .reset_index()
    )
    OUT_FILE = Paths.OUT_DATA / f"distances_{name}.parquet"
    distances.to_pandas().to_parquet(OUT_FILE, index=False)


def main():
    nodes = cudf.from_pandas(
        pd.read_parquet(Paths.PROCESSED / "oproad" / "nodes.parquet")
    )
    edges = cudf.from_pandas(
        pd.read_parquet(Paths.PROCESSED / "oproad" / "edges.parquet")
    )
    postcodes = cudf.from_pandas(
        pd.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")
    )

    poi_list = list(Paths.PROCESSED.glob("*.parquet"))

    for poi in tqdm(poi_list):
        df = cudf.from_pandas(pd.read_parquet(poi))
        name = poi.stem
        print(f"Processing {name}")
        if (Paths.OUT_DATA / f"distances_{name}.parquet").exists():
            continue
        _routing(df, name, postcodes, nodes, edges)


if __name__ == "__main__":
    main()
