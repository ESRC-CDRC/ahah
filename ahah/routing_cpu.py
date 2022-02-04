from __future__ import annotations

import dask.dataframe as dd
import networkx as nx
import numpy as np
import pandas as pd
import time
from ahah.common.logger import logger
from ahah.common.utils import Config
from rich.progress import track


class CPURouting:
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
    edges : pd.DataFrame
        Dataframe containing road edges
    nodes : pd.DataFrame
        Dataframe containing road nodes
    postcodes : pd.DataFrame
        Dataframe containing all postcodes
    pois : pd.DataFrame
        Dataframe containing all POIs
    weights : str
        Graph weights to use, e.g. `time_weighted` or `distance`
    """

    def __init__(
        self,
        name: str,
        edges: pd.DataFrame,
        nodes: pd.DataFrame,
        postcodes: pd.DataFrame,
        pois: pd.DataFrame,
        weights: str = "time_weighted",
        buffer: int = 50_000,
    ):
        self.postcode_ids: np.ndarray = postcodes["node_id"].unique()
        self.pois = pois.drop_duplicates("node_id")

        self.edges = edges
        self.nodes = nodes
        self.name = name
        self.weights = weights
        self.buffer = buffer

        if not self.buffer:
            self.graph = nx.Graph()
            self.graph.add_weighted_edges_from(
                self.edges[["source", "target", self.weights]]
                .to_records(index=False)
                .tolist()
            )

        self.log_file = Config.OUT_DATA / "logs" / f"{self.name}_intermediate.csv"

        if self.log_file.exists():
            logger.warning("Resuming from a previous run.")
            self.idx = pd.read_csv(self.log_file)["idx"].max() - 1
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
        self.log_file.unlink()

    def create_sub_graph(self, poi: pd.Series):
        min_pt = np.array([poi.easting - self.buffer, poi.northing - self.buffer])
        max_pt = np.array([poi.easting + self.buffer, poi.northing + self.buffer])

        node_subset = self.nodes[
            np.all(
                (min_pt <= self.nodes[["easting", "northing"]].values)
                & (self.nodes[["easting", "northing"]].values <= max_pt),
                axis=1,
            )
        ]

        sub_edges = self.edges[
            self.edges["source"].isin(node_subset["node_id"])
            | self.edges["target"].isin(node_subset["node_id"])
        ]
        sub_graph = nx.Graph()
        sub_graph.add_weighted_edges_from(
            sub_edges[["source", "target", self.weights]]
            .to_records(index=False)
            .tolist()
        )
        return sub_graph

    def get_shortest_dists(self, poi):
        """
        Use `cugraph.sssp` to calculate shortest paths from POI to postcodes

        First subsets road graph, then finds shortest paths, ensuring all paths are
        routed that are known to be important to each POI.

        Parameters
        ----------
        poi : namedtuple
            Single POI created from `df.itertuples()`
        """
        if self.buffer:
            self.graph = self.create_sub_graph(poi=poi)

        shortest_paths: pd.DataFrame = nx.single_source_dijkstra_path_length(
            self.graph, source=poi.node_id
        )
        shortest_paths = pd.DataFrame(
            {"vertex": shortest_paths.keys(), "distance": shortest_paths.values()}
        )
        pc_dist = shortest_paths[shortest_paths["vertex"].isin(self.postcode_ids)]

        self.idx += 1
        pc_dist["idx"] = self.idx

        if self.log_file.exists():
            self.distances = pd.read_csv(self.log_file).append(pc_dist)
        else:
            self.distances = pc_dist[["vertex", "distance", "idx"]]

        self.distances = (
            self.distances.sort_values("distance")
            .drop_duplicates("vertex")
            .reset_index()[["vertex", "distance", "idx"]]
        )
        self.distances.to_csv(self.log_file, index=False)
