import cudf
import geopandas as gpd

import pandas as pd
from cuml.neighbors.nearest_neighbors import NearestNeighbors
from pandas import IndexSlice as idx
from rich.progress import track

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
    edges = edges.set_index(["formOfWay", "routeHierarchy"])

    edges.loc[idx[:, "Motorway"], "speed_estimate"] = 67
    edges.loc[idx["Dual Carriageway", a_roads], "speed_estimate"] = 57
    edges.loc[idx["Dual Carriageway", b_roads], "speed_estimate"] = 45
    edges.loc[idx["Single Carriageway", a_roads + b_roads], "speed_estimate"] = 25
    edges.loc[idx[:, "Minor Road"], "speed_estimate"] = 24
    edges.loc[idx[:, "Local Road"], "speed_estimate"] = 20
    edges.loc[idx["Roundabout", :], "speed_estimate"] = 10
    edges.loc[idx[["Track", "Layby"], :], "speed_estimate"] = 5
    edges.loc[edges["speed_estimate"] == -1, "speed_estimate"] = 10

    # Unsure what to keep
    # edges = edges.drop(
    #     index="Traffic Island Link At Junction", level=0, errors="ignore"
    # )
    # edges = edges.drop(index="Traffic Island Link", level=0, errors="ignore")
    # edges = edges.drop(index="Enclosed Traffic Area", level=0, errors="ignore")
    # edges = edges.drop(index="Layby", level=0, errors="ignore")
    # edges = edges.drop(index="Track", level=0, errors="ignore")
    # edges = edges.drop(index="Guided Busway", level=0, errors="ignore")
    # edges = edges.drop(index="Restricted Local Access Road", level=1, errors="ignore")
    # edges = edges.drop(
    #     index="Restricted Secondary Access Road", level=1, errors="ignore"
    # )

    edges = edges.assign(
        speed_estimate=edges["speed_estimate"] * 1.609344,
        time_weighted=(edges["length"].astype(float) / 1000)
        / edges["speed_estimate"]
        * 60,
    )

    return edges[["startNode", "endNode", "time_weighted", "length"]]


def change_ferry_nodes(nodes, fnodes, fedges):
    nbrs = NearestNeighbors(n_neighbors=1, output_type="cudf", algorithm="brute").fit(
        nodes[["easting", "northing"]]
    )
    _, indices = nbrs.kneighbors(fnodes[["easting", "northing"]])
    fnodes["road_id"] = nodes.iloc[indices]["TOID"].reset_index(drop=True)

    fedges = (
        fedges.merge(
            fnodes[["TOID", "road_id"]],
            left_on="startNode",
            right_on="TOID",
        )
        .rename(columns={"road_id": "startNode"})
        .drop("TOID", axis=1)
    )
    fedges = (
        fedges.merge(
            fnodes[["TOID", "road_id"]],
            left_on="endNode",
            right_on="TOID",
        )
        .rename(columns={"road_id": "endNode"})
        .drop("TOID", axis=1)
    )

    fnodes = fnodes[["road_id", "easting", "northing"]].rename(
        columns={"road_ID": "TOID"}
    )
    return fnodes, fedges


if __name__ == "__main__":
    logger.info("Starting OS highways processing...")

    NUM_EDGES = 5_062_741
    NUM_NODES = 4_289_045

    # edges processing
    edges = cudf.DataFrame()
    for n in track(range(0, NUM_EDGES, 100_000), description="Processing edges..."):
        subset_edges = gpd.read_file(
            Config.HW_DATA,
            layer="RoadLink",
            rows=slice(n, n + 100_000),
            ignore_geometry=True,
        ).pipe(process_edges)
        edges: cudf.DataFrame = edges.append(cudf.from_pandas(subset_edges))
    ferry_edges = cudf.from_pandas(
        gpd.read_file(Config.HW_DATA, layer="FerryLink", ignore_geometry=True)
    )[["startNode", "endNode", "SHAPE_Length"]].rename(
        columns={"SHAPE_Length": "length"}
    )
    ferry_edges = ferry_edges.assign(
        time_weighted=(ferry_edges["length"].astype(float) / 1000) / 25 * 1.609344 * 60
    )
    logger.debug("Edges processed.")

    # nodes processing
    nodes = cudf.DataFrame()
    for n in track(range(0, NUM_NODES, 100_000), description="Processing nodes..."):
        subset_nodes = gpd.read_file(
            Config.HW_DATA,
            layer="RoadNode",
            rows=slice(n, n + 100_000),
        )
        subset_nodes["easting"], subset_nodes["northing"] = (
            subset_nodes.geometry.x.astype("int"),
            subset_nodes.geometry.y.astype("int"),
        )
        subset_nodes.drop("geometry", axis=1, inplace=True)
        nodes: cudf.DataFrame = nodes.append(cudf.from_pandas(subset_nodes))
    ferry_nodes = gpd.read_file(Config.HW_DATA, layer="FerryNode")[["TOID", "geometry"]]
    ferry_nodes["easting"], ferry_nodes["northing"] = (
        ferry_nodes.geometry.x.astype("int"),
        ferry_nodes.geometry.y.astype("int"),
    )
    ferry_nodes = cudf.from_pandas(ferry_nodes.drop("geometry", axis=1))
    logger.debug("Nodes processed.")

    ferry_nodes, ferry_edges = change_ferry_nodes(nodes, ferry_nodes, ferry_edges)

    nodes = nodes[["TOID", "easting", "northing"]].append(ferry_nodes)
    edges = edges.reset_index(drop=True).append(ferry_edges)
    nodes = nodes.rename(columns={"TOID": "node_id"})
    nodes = nodes[
        (nodes["node_id"].isin(edges["startNode"]))
        | (nodes["node_id"].isin(edges["endNode"]))
    ]
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
