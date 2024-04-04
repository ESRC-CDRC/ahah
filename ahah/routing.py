from __future__ import annotations

import time
import warnings
from typing import NamedTuple

import cudf
import cugraph
import cupy
import cuspatial
import geopandas as gpd
import numpy as np
import pandas as pd
from rich.progress import track

from ahah.common.logger import logger
from ahah.common.utils import Config

cupy.cuda.Device(1).use()


class Routing:
    """
    Main class for calculating routing from POI to postcodes within a road network.

    Primarily uses `cugraph` to GPU accelerate routing. While the interest is distance
    from postcodes to POI, this class does routing from POI to postcodes, appending to
    a large intermediate file. When complete the routing takes the minimum distance
    for each postcode.

    Parameters
    ----------
    name : str
        Name of POI
    edges : cudf.DataFrame
        Dataframe containing road edges
    nodes : cudf.DataFrame
        Dataframe containing road nodes
    postcodes : cudf.DataFrame
        Dataframe containing all postcodes
    pois : pd.DataFrame
        Dataframe containing all POIs
    weights : str
        Graph weights to use, e.g. `time_weighted` or `distance`
    """

    def __init__(
        self,
        name: str,
        edges: cudf.DataFrame,
        nodes: cudf.DataFrame,
        postcodes: cudf.DataFrame,
        pois: pd.DataFrame,
        weights: str = "time_weighted",
        buffer: int = 5_000,
    ):
        self.postcode_ids: np.ndarray = postcodes["node_id"].unique().to_numpy()
        self.pois = pois.drop_duplicates("node_id")

        self.edges = edges
        self.nodes = nodes
        self.nodes_nogeo = cudf.DataFrame(nodes.drop("geometry", axis=1))
        self.name = name
        self.weights = weights
        self.buffer = buffer

        self.graph = cugraph.Graph()
        self.graph.from_cudf_edgelist(
            self.edges,
            source="source",
            destination="target",
            edge_attr=self.weights,
            # weights=self.weights,
            renumber=False,
        )

        self.idx = 0
        self.distances = cudf.DataFrame()

    def fit(self) -> None:
        """
        Iterate and apply routing to each POI

        This function primarily allows for the intermediate steps in routing to be
        logged. This means that if the routing is stopped midway it can be restarted.
        """
        t1 = time.time()
        for poi in track(
            self.pois.itertuples(),
            description=f"Processing {self.name}...",
            total=len(self.pois),
        ):
            self.get_shortest_dists(poi)
        t2 = time.time()
        tdiff = t2 - t1
        logger.debug(
            f"Routing complete for {self.name} in {tdiff / 60:.2f} minutes,"
            " finding minimum distances."
        )
        # self.log_file.unlink()

    def create_sub_graph(self, poi) -> cugraph.Graph:
        """
        Create a subgraph of road nodes based on buffer distance

        The subgraph is created using euclidean distance and
        `cuspatial.points_in_spatial_window`. If buffers are not large enough to
        include all nodes identified as important to that particular POI, it is
        increased in size.

        Parameters
        ----------
        poi : namedtuple
            Single POI created by `df.itertuples()`

        Returns
        -------
        cugraph.Graph:
            Graph object that is a subset of all road nodes
        """
        buffer = max(poi.buffer, self.buffer)
        while True:
            node_subset = cuspatial.points_in_spatial_window(
                points=nodes["geometry"],
                min_x=poi.easting - buffer,
                max_x=poi.easting + buffer,
                min_y=poi.northing - buffer,
                max_y=poi.northing + buffer,
            )
            node_subset = cudf.DataFrame(
                {"easting": node_subset.points.x, "northing": node_subset.points.y}
            ).merge(self.nodes_nogeo, on=["easting", "northing"])
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                sub_graph = cugraph.subgraph(self.graph, node_subset["node_id"])

            if sub_graph is None:
                continue

            pc_nodes = cudf.Series(poi.pc_node).isin(sub_graph.nodes()).sum()
            poi_node = sub_graph.nodes().isin([poi.node_id]).sum()

            # ensure all postcode nodes in + poi node
            # don't incrase buffer for large pois lists
            if poi_node & (pc_nodes == len(poi.pc_node)):
                return sub_graph
            buffer = buffer * 2
            # logger.debug(f"{poi.Index=}: increasing {buffer=}")

    def get_shortest_dists(self, poi: NamedTuple) -> None:
        """
        Use `cugraph.sssp` to calculate shortest paths from POI to postcodes

        First subsets road graph, then finds shortest paths, ensuring all paths are
        routed that are known to be important to each POI. Saves to `hdf` to allow
        restarts.

        Parameters
        ----------
        poi : namedtuple
            Single POI created from `df.itertuples()`
        """
        if self.buffer:
            sub_graph = self.create_sub_graph(poi=poi)
        else:
            sub_graph = self.graph

        shortest_paths: cudf.DataFrame = cugraph.filter_unreachable(
            cugraph.sssp(sub_graph, source=poi.node_id)
        )
        pc_dist = shortest_paths[shortest_paths.vertex.isin(self.postcode_ids)]

        pc_dist["idx"] = self.idx

        if idx != 0:
            self.distances = cudf.concat([self.distances, pc_dist])
        else:
            self.distances = pc_dist[["vertex", "distance", "idx"]]

        self.distances = (
            self.distances.sort_values("distance")
            .drop_duplicates("vertex")
            .reset_index()[["vertex", "distance", "idx"]]
        )
        self.idx += 1

        # logger.debug(
        #     f"Current disances for {self.name} {len(self.distances)} "
        #     f"out of {len(self.postcode_ids)} postcode nodes."
        # )


if __name__ == "__main__":
    logger.info("Starting Routing!")
    logger.debug("Reading graph and postcodes.")

    edges = cudf.from_pandas(pd.read_parquet(Config.OS_GRAPH / "edges.parquet"))
    edges["time_weighted"] = edges["time_weighted"].astype("float32")

    nodes = pd.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    nodes = cuspatial.from_geopandas(
        gpd.GeoDataFrame(
            nodes, geometry=gpd.points_from_xy(nodes["easting"], nodes["northing"])
        )
    )

    postcodes = cudf.from_pandas(
        pd.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet")
    )

    logger.debug("Finished reading nodes, edges and postcodes.")

    logger.debug(f"Starting Routing for {Config.POI_LIST}.")
    for idx, poi in enumerate(Config.POI_LIST):
        df = pd.read_parquet(Config.PROCESSED_DATA / f"{poi}.parquet").reset_index(
            drop=True
        )
        logger.debug(f"Starting routing for {poi} ({idx+1}/{len(Config.POI_LIST)}).")
        OUT_FILE = Config.OUT_DATA / f"distances_{poi}.csv"

        if not OUT_FILE.exists():
            routing = Routing(
                name=poi,
                edges=edges,
                nodes=nodes,
                postcodes=postcodes,
                pois=df,
                weights="time_weighted",
            )
            routing.fit()

            distances = (
                routing.distances.set_index("vertex")
                .join(postcodes.set_index("node_id"), how="right")
                .reset_index()
            )

            logger.debug(f"Saving distances for {poi} to {OUT_FILE}.")
            distances.to_pandas()[["postcode", "distance"]].to_csv(
                OUT_FILE, index=False
            )
        else:
            logger.warning(f"{OUT_FILE} exists! Skipping {poi}.")
