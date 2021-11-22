import cudf
import geopandas as gpd
import pandas as pd
from ahah.common.logger import logger
from ahah.common.utils import Config
from pandas import IndexSlice as idx
from rich.progress import track


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

    edges = edges.assign(
        speed_estimate=edges["speed_estimate"] * 1.609344,
        time_weighted=(edges["length"].astype(float) / 1000)
        / edges["speed_estimate"]
        * 60,
    )

    return edges[["startNode", "endNode", "time_weighted", "length"]]


if __name__ == "__main__":
    logger.info("Starting OS highways processing...")

    NUM_EDGES = 5_062_741
    NUM_NODES = 4_289_045

    # for both edges and nodes I subset by 100k rows as they are too large to
    # read directly into memory. There may be a better way.

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
    logger.debug("Nodes processed.")

    nodes["node_id"] = nodes["TOID"]

    nodes["node_id"] = nodes["node_id"].astype("category")
    node_ids = dict(enumerate(nodes["node_id"].cat.categories.to_pandas()))
    node_ids = {v: k for k, v in node_ids.items()}
    nodes["node_id"] = nodes["node_id"].astype("str").map(node_ids).astype(int)

    edges["source"] = edges["startNode"].map(node_ids).astype(int)
    edges["target"] = edges["endNode"].map(node_ids).astype(int)

    nodes[Config.NODE_COLS].to_parquet(Config.OS_GRAPH / "nodes.parquet", index=False)
    logger.debug(f"Nodes saved to {Config.OS_GRAPH / 'nodes.parquet'}")
    edges.reset_index()[Config.EDGE_COLS].to_parquet(Config.OS_GRAPH / "edges.parquet", index=False)
    logger.debug(f"Edges saved to {Config.OS_GRAPH / 'edges.parquet'}")
