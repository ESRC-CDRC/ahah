from __future__ import annotations

import cudf
import cugraph
import cuspatial
import dask.dataframe as dd
import numpy as np
import pandas as pd
import time
from ahah.common.logger import logger
from ahah.common.utils import Config
from rich.progress import track


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
    ):
        self.postcode_ids: np.ndarray = postcodes["node_id"].unique().to_array()
        self.pois = pois.drop_duplicates("node_id")

        self.edges = edges
        self.nodes = nodes
        self.name = name
        self.weights = weights

        self.log_file = Config.OUT_DATA / "logs" / f"{self.name}_intermediate.h5"

        if self.log_file.exists():
            logger.warning("Resuming from a previous run.")
            # -1 in case program stopped while saving to hdf
            self.idx = (
                dd.read_hdf(self.log_file, key=self.name)["idx"].max().compute() - 1
            )
            logger.warning(
                f"Run resumed at {self.idx / len(self.pois) * 100:.2f}% ({self.idx} / {len(self.pois)})"
            )
        else:
            self.idx = 0

    def fit(self) -> None:
        """
        Iterate and apply routing to each POI

        This function primarily allows for the intermediate steps in routing to be
        logged. This means that if the routing is stopped midway it can be restarted.
        """
        t1 = time.time()
        for poi in track(
            self.pois.iloc[self.idx :].itertuples(),
            description=f"Processing {self.name}...",
            total=len(self.pois) - self.idx,
        ):
            self.get_shortest_dists(poi)
        t2 = time.time()
        tdiff = t2 - t1
        logger.debug(
            f"Routing complete for {self.name} in {tdiff / 60:.2f} minutes,"
            " finding minimum distances."
        )
        t1 = time.time()
        self.distances = (
            dd.read_hdf(
                self.log_file,
                key=f"{self.name}",
                columns=["vertex", "distance"],
            )
            .map_partitions(cudf.from_pandas)
            .groupby("vertex")
            .min()
            .compute()
        )
        t2 = time.time()
        tdiff = t2 - t1
        logger.debug(
            f"Found minimum distances for {self.name} in {tdiff / 60:.2f} minutes."
        )
        self.log_file.unlink()

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
        # # very small buffers do not work well
        buffer = max(poi.buffer, 1000)
        while True:
            node_subset = cuspatial.points_in_spatial_window(
                min_x=poi.easting - buffer,
                max_x=poi.easting + buffer,
                min_y=poi.northing - buffer,
                max_y=poi.northing + buffer,
                xs=self.nodes["easting"],
                ys=self.nodes["northing"],
            )
            node_subset = node_subset.merge(
                self.nodes, left_on=["x", "y"], right_on=["easting", "northing"]
            ).drop(["x", "y"], axis=1)
            sub_source = self.edges.merge(
                node_subset, left_on="source", right_on="node_id"
            )
            sub_target = self.edges.merge(
                node_subset, left_on="target", right_on="node_id"
            )
            sub_edges = sub_source.append(sub_target).drop_duplicates(
                ["source", "target"]
            )
            sub_graph = cugraph.Graph()
            sub_graph.from_cudf_edgelist(
                sub_edges,
                source="source",
                destination="target",
                edge_attr=self.weights,
            )

            pc_nodes = cudf.Series(poi.pc_node).isin(sub_graph.nodes()).sum()
            poi_node = sub_graph.nodes().isin([poi.node_id]).sum()

            # ensure all postcode nodes in + poi node
            if (poi_node) & (pc_nodes == len(poi.pc_node)):
                return sub_graph
            buffer = (buffer + 100) * 2
            logger.debug(f"{poi.Index=}: increasing {buffer=}")

    def get_shortest_dists(self, poi):
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
        self.graph = self.create_sub_graph(poi=poi)

        shortest_paths: cudf.DataFrame = cugraph.filter_unreachable(
            cugraph.sssp(self.graph, source=poi.node_id)
        )
        pc_dist = shortest_paths[shortest_paths.vertex.isin(self.postcode_ids)]

        self.idx += 1
        pc_dist["idx"] = self.idx
        pc_dist.to_hdf(
            self.log_file,
            key=self.name,
            format="table",
            append=True,
            index=False,
        )


if __name__ == "__main__":
    logger.info("Starting Routing!")
    logger.debug("Reading graph and postcodes.")

    edges = cudf.read_parquet(Config.OS_GRAPH / "edges.parquet")
    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    postcodes = cudf.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet")

    logger.debug("Finished reading nodes, edges and postcodes.")

    logger.debug(f"Starting Routing for {Config.POI_LIST}.")
    for idx, poi in enumerate(Config.POI_LIST):
        df = pd.read_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")
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

            distances = routing.distances.join(
                postcodes.set_index("node_id")
            ).reset_index()

            logger.debug(f"Saving distances for {poi} to {OUT_FILE}.")
            distances[["postcode", "distance"]].to_csv(OUT_FILE, index=False)
        else:
            logger.warning(f"{OUT_FILE} exists! Skipping {poi}.")
