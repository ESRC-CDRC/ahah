from tqdm import tqdm

import warnings
from typing import NamedTuple

import cudf
import cugraph
import cupy as cp


class Routing:
    def __init__(
        self,
        edges: cudf.DataFrame,
        nodes: cudf.DataFrame,
        inputs: cudf.DataFrame,
        outputs: cudf.DataFrame,
        weights: str = "time_weighted",
        min_buffer: int = 5_000,
        max_buffer: int = 1_000_000,
        cutoff: int | None = None,
    ):
        self.inputs: cudf.DataFrame = inputs
        self.outputs: cudf.DataFrame = outputs

        self.road_edges: cudf.DataFrame = edges
        self.road_nodes: cudf.GeoDataFrame = nodes
        self.weights: str = weights
        self.min_buffer: int = min_buffer
        self.max_buffer: int = max_buffer
        self.cutoff: int = cutoff

        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            self.graph = cugraph.Graph()
            self.graph.from_cudf_edgelist(
                self.road_edges,
                source="start_node",
                destination="end_node",
                edge_attr=self.weights,
                renumber=False,
            )

        self.distances: cudf.DataFrame = cudf.DataFrame()

    def fit(self) -> None:
        for item in tqdm(self.inputs.itertuples(), total=len(self.inputs)):
            self.get_shortest_dists(item)

    def create_sub_graph(self, item) -> cugraph.Graph:
        buffer = max(self.min_buffer, item.buffer)
        while True:
            nodes_subset = self.road_nodes.copy()
            nodes_subset["distance"] = cp.sqrt(
                (nodes_subset["easting"] - item.easting) ** 2
                + (nodes_subset["northing"] - item.northing) ** 2
            )
            nodes_subset = (
                nodes_subset[nodes_subset["distance"] <= buffer]["node_id"]
                .unique()
                .to_pandas()
                .tolist()
            )
            edges_subset = self.road_edges.copy()
            edges_subset = edges_subset[
                edges_subset["start_node"].isin(nodes_subset)
                | edges_subset["end_node"].isin(nodes_subset)
            ]

            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                sub_graph = cugraph.Graph()
                sub_graph.from_cudf_edgelist(
                    edges_subset,
                    source="start_node",
                    destination="end_node",
                    edge_attr=self.weights,
                )
                main_sub_graph = self._remove_partial_graphs(sub_graph, edges_subset)

            if main_sub_graph is None:
                if buffer >= self.max_buffer:
                    return sub_graph
                buffer = buffer * 2
                continue

            ntarget_nds = cudf.Series(item.top_nodes).isin(main_sub_graph.nodes()).sum()
            df_node = item.node_id in main_sub_graph.nodes().to_arrow().to_pylist()

            if (
                df_node & (ntarget_nds == len(item.top_nodes))
                or buffer >= self.max_buffer
            ):
                return sub_graph
            buffer = buffer * 2

    def _remove_partial_graphs(self, sub_graph, edges_subset):
        components = cugraph.connected_components(sub_graph)
        largest_component_label = components["labels"].mode()[0]

        largest_component_nodes = set(
            components[components["labels"] == largest_component_label]["vertex"]
            .to_pandas()
            .to_list()
        )
        filtered_edges = edges_subset[
            edges_subset["start_node"].isin(largest_component_nodes)
            | edges_subset["end_node"].isin(largest_component_nodes)
        ]
        main_sub_graph = cugraph.Graph()
        main_sub_graph.from_cudf_edgelist(
            filtered_edges,
            source="start_node",
            destination="end_node",
            edge_attr=self.weights,
        )
        return main_sub_graph

    def get_shortest_dists(self, item: NamedTuple) -> None:
        sub_graph = self.create_sub_graph(item=item)
        if sub_graph is None:
            return
        spaths: cudf.DataFrame = cugraph.filter_unreachable(
            cugraph.sssp(sub_graph, source=item.node_id, cutoff=self.cutoff)
        )
        dist = spaths[spaths.vertex.isin(self.outputs["node_id"])]
        self.distances = cudf.concat([self.distances, dist])
        self.distances = (
            self.distances.sort_values("distance")
            .drop_duplicates("vertex")
            .reset_index()[["vertex", "distance"]]
        )
