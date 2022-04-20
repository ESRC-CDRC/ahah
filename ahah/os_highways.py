import cudf
import geopandas as gpd
import pandas as pd
from cuml.neighbors.nearest_neighbors import NearestNeighbors
from pandas import IndexSlice as idx

from ahah.common.logger import logger
from ahah.common.utils import Config


def process_edges(edges: pd.DataFrame) -> pd.DataFrame:
    """
    Create time estimates for road edges based on OS documentation

    Time estimates based on speed estimates and edge length. Speed estimates
    taken from OS documentation. This also filters to remove extra cols.

    Parameters
    ----------
    edges : pd.DataFrame
        OS highways df containing edges, and other metadata

    Returns
    -------
    pd.DataFrame:
        OS highways df with time weighted estimates
    """
    a_roads = ["A Road", "A Road Primary"]
    b_roads = ["B Road", "B Road Primary"]

    edges["speed_estimate"] = -1
    edges = edges.set_index(["formOfWay", "roadClassification"])

    edges.loc[idx[:, "Motorway"], "speed_estimate"] = 67
    edges.loc[idx["Dual Carriageway", a_roads], "speed_estimate"] = 57
    edges.loc[idx["Dual Carriageway", b_roads], "speed_estimate"] = 45
    edges.loc[idx["Single Carriageway", a_roads + b_roads], "speed_estimate"] = 25
    edges.loc[idx[:, "Unclassified"], "speed_estimate"] = 24
    edges.loc[idx["Roundabout", :], "speed_estimate"] = 10
    edges.loc[idx[["Track", "Layby"], :], "speed_estimate"] = 5
    edges.loc[edges["speed_estimate"] == -1, "speed_estimate"] = 10

    edges = edges.assign(
        speed_estimate=edges["speed_estimate"] * 1.609344,
        time_weighted=(edges["length"].astype(float) / 1000)
        / edges["speed_estimate"]
        * 60,
    )

    return edges[["startNode", "endNode", "time_weighted", "length"]]


def process_ferry(ferry_df):
    ferry_df["node_id"] = ferry_df.geometry.boundary.apply(lambda row: row.geoms)

    ferry_nodes = (
        ferry_df["node_id"]
        .explode()
        .drop_duplicates()
        .reset_index(drop=True)
        .to_frame()
    )
    ferry_nodes["easting"] = ferry_nodes["node_id"].apply(lambda row: row.x)
    ferry_nodes["northing"] = ferry_nodes["node_id"].apply(lambda row: row.y)

    ferry_edges = ferry_df[["node_id", "geometry"]].copy()
    ferry_edges["startNode"] = ferry_edges["node_id"].apply(lambda row: row[0])
    ferry_edges["endNode"] = ferry_edges["node_id"].apply(lambda row: row[1])
    ferry_edges["length"] = ferry_edges.geometry.length
    ferry_edges = ferry_edges.assign(
        time_weighted=(ferry_edges["length"].astype(float) / 1000) / 25 * 1.609344 * 60
    )

    ferry_nodes["node_id"] = ferry_nodes["node_id"].astype(str)
    ferry_nodes = cudf.from_pandas(ferry_nodes[["node_id", "easting", "northing"]])
    ferry_edges["startNode"] = ferry_edges["startNode"].astype(str)
    ferry_edges["endNode"] = ferry_edges["endNode"].astype(str)
    ferry_edges = cudf.from_pandas(
        ferry_edges.rename(columns={"FERRY_FROM": "startNode", "FERRY_TO": "endNode"})[
            ["startNode", "endNode", "length", "time_weighted"]
        ]
    )
    return ferry_nodes, ferry_edges


def change_ferry_nodes(nodes_df, fnodes, fedges):
    nbrs = NearestNeighbors(n_neighbors=1, output_type="cudf", algorithm="brute").fit(
        nodes_df[["easting", "northing"]]
    )
    _, indices = nbrs.kneighbors(fnodes[["easting", "northing"]])
    fnodes["road_id"] = nodes_df.iloc[indices]["node_id"].reset_index(drop=True)

    fedges = (
        fedges.merge(
            fnodes[["node_id", "road_id"]],
            left_on="startNode",
            right_on="node_id",
        )
        .rename(columns={"road_id": "startNode"})
        .drop("node_id", axis=1)
    )

    fedges = (
        fedges.merge(
            fnodes[["node_id", "road_id"]],
            left_on="endNode",
            right_on="node_id",
        )
        .rename(columns={"road_id": "endNode"})
        .drop("node_id", axis=1)
    )

    fnodes = fnodes[["road_id", "easting", "northing"]].rename(
        columns={"road_id": "node_id"}
    )
    return fnodes, fedges


if __name__ == "__main__":
    logger.info("Starting OS highways processing...")

    edges = cudf.from_pandas(
        gpd.read_file(
            Config.HW_DATA,
            layer="RoadLink",
            ignore_geometry=True,
        ).pipe(process_edges)
    )
    nodes = gpd.read_file(Config.HW_DATA, layer="RoadNode")
    nodes["easting"], nodes["northing"] = nodes.geometry.x, nodes.geometry.y
    nodes = cudf.from_pandas(
        nodes[["id", "easting", "northing"]].rename(columns={"id": "node_id"})
    )
    ferry = gpd.read_file(
        "./data/raw/os_highways/strtgi_essh_gb/ferry_line.shp",
    )[["FERRY_FROM", "FERRY_TO", "geometry"]]
    ferry_nodes, ferry_edges = process_ferry(ferry)

    # for some reason the isles of scilly do not have a ferry route
    extra_ferry_nodes = {
        "node_id": ["scilly", "penz"],
        "easting": [90139, 147432],
        "northing": [10633, 30086],
    }
    extra_ferry_edges = {
        "startNode": ["penz", "scilly"],
        "endNode": ["scilly", "penz"],
        "length": [165, 165],
        "time_weighted": [165, 165],
    }
    ferry_nodes = ferry_nodes.append(extra_ferry_nodes, ignore_index=True)
    ferry_edges = ferry_edges.append(extra_ferry_edges, ignore_index=True)

    ferry_nodes, ferry_edges = change_ferry_nodes(nodes, ferry_nodes, ferry_edges)

    nodes = nodes[["node_id", "easting", "northing"]].append(ferry_nodes)
    edges = edges.reset_index(drop=True).append(ferry_edges)

    # convert to sequential ints
    nodes["node_id"] = nodes["node_id"].astype("category")
    node_ids = dict(enumerate(nodes["node_id"].cat.categories.to_pandas()))
    node_ids = {v: k for k, v in node_ids.items()}
    nodes["node_id"] = nodes["node_id"].astype("str").map(node_ids).astype(int)

    edges["source"] = edges["startNode"].map(node_ids).astype(int)
    edges["target"] = edges["endNode"].map(node_ids).astype(int)

    nodes[Config.NODE_COLS].to_parquet(Config.OS_GRAPH / "nodes.parquet", index=False)
    logger.debug(f"Nodes saved to {Config.OS_GRAPH / 'nodes.parquet'}")
    edges.reset_index()[Config.EDGE_COLS].to_parquet(
        Config.OS_GRAPH / "edges.parquet", index=False
    )
    logger.debug(f"Edges saved to {Config.OS_GRAPH / 'edges.parquet'}")
