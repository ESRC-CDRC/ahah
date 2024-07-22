import cudf
import cupy as cp
import numpy as np
import pandas as pd
from scipy.spatial import KDTree


def add_to_graph(df, nodes, edges, k=10):
    nodes_tree = KDTree(nodes[["easting", "northing"]].values.get())
    distances, indices = nodes_tree.query(df[["easting", "northing"]].values, k=k)

    nearest_nodes_df = pd.DataFrame(
        {
            "nearest_node": nodes.iloc[indices.flatten()]["node_id"]
            .reset_index(drop=True)
            .to_numpy(),
            "distance": distances.flatten() + 0.01,
        }
    )

    new_node_ids = cp.arange(len(nodes) + 1, len(nodes) + 1 + len(df))
    df["node_id"] = new_node_ids.get()
    new_nodes = df[["node_id", "easting", "northing"]]
    nodes = cudf.concat([nodes, cudf.from_pandas(new_nodes)])
    new_edges = cudf.DataFrame(
        {
            "start_node": df.loc[np.repeat(df.index, k)].reset_index(drop=True)[
                "node_id"
            ],
            "end_node": nearest_nodes_df["nearest_node"],
            "length": nearest_nodes_df["distance"],
        }
    )
    new_edges["time_weighted"] = (
        (new_edges["length"].astype(float) / 1000) / 25 * 1.609344 * 60
    )
    edges = cudf.concat([edges, new_edges])

    return (
        df.reset_index(drop=True),
        nodes.reset_index(drop=True),
        edges.reset_index(drop=True),
    )


def add_topk(input, output, k=10):
    df_tree = KDTree(input[["easting", "northing"]].values)
    distances, indices = df_tree.query(output[["easting", "northing"]].values, k=k)

    indices = pd.DataFrame(indices)
    input = (
        pd.concat([output.reset_index(drop=True), indices], axis=1)[
            ["node_id"] + indices.columns.tolist()
        ]
        .set_index("node_id")
        .stack()
        .rename("df_idx")
        .reset_index()
        .rename(columns={"node_id": "top_nodes"})
        .drop("level_1", axis=1)
        .dropna()
        .groupby("df_idx")
        .agg(list)
        .join(input, how="right")
    )
    input["top_nodes"] = input["top_nodes"].apply(
        lambda row: list(set(row)) if isinstance(row, list) else row
    )
    distances = pd.DataFrame(distances).stack().rename("buffer").reset_index()
    indices = indices.stack().rename("node_id").reset_index()

    buffers = (
        pd.DataFrame(
            {
                "node_id": input.iloc[indices["node_id"].values]["node_id"],
                "buffer": distances["buffer"].values,
            }
        )
        .sort_values("buffer", ascending=False)
        .drop_duplicates("node_id")
    )
    input = input[input["top_nodes"].apply(lambda x: isinstance(x, list))]

    return input.merge(buffers, on="node_id", how="left").dropna()
