import cudf
import functools
import matplotlib.pyplot as plt
import time
from ahah.common.utils import Config
from ahah.process_routing import get_buffers, nearest_nodes
from ahah.routing import Routing
from ahah.routing_cpu import CPURouting


def timefunc(func):
    @functools.wraps(func)
    def time_closure(*args, **kwargs):
        """time_wrapper's doc string"""
        start = time.perf_counter()
        result = func(*args, **kwargs)
        time_elapsed = time.perf_counter() - start
        print(f"Function: {func.__name__}, Time: {time_elapsed}")
        return result

    return time_closure


def get_data():
    edges = cudf.read_parquet(Config.OS_GRAPH / "edges.parquet")
    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    postcodes = cudf.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet")
    postcodes = postcodes[postcodes["postcode"].str.match(r"^L\d.*")]

    nodes = nodes[
        (nodes["easting"] >= postcodes["easting"].min())
        & (nodes["easting"] <= postcodes["easting"].max())
        & (nodes["northing"] >= postcodes["northing"].min())
        & (nodes["northing"] <= postcodes["northing"].max())
    ]
    edges = edges[
        edges["source"].isin(nodes["node_id"]) | edges["target"].isin(nodes["node_id"])
    ]

    gpp = cudf.read_parquet(Config.PROCESSED_DATA / "gpp.parquet").drop(
        ["buffer", "pc_node"], axis=1
    )
    gpp = gpp[gpp["node_id"].isin(nodes["node_id"].to_pandas())]

    gpp = nearest_nodes(gpp.reset_index(drop=True), nodes=nodes)
    gpp = get_buffers(poi=gpp, postcodes=postcodes.reset_index(drop=True), k=10)
    return {
        "name": "gpp",
        "edges": edges,
        "nodes": nodes,
        "postcodes": postcodes,
        "pois": gpp.to_pandas(),
        "buffer": False,
    }


@timefunc
def gpu_liverpool(**kwargs):
    routing = Routing(**kwargs)
    routing.fit()
    postcodes = kwargs.get("postcodes")
    distances = routing.distances.join(postcodes.set_index("node_id")).reset_index()
    return distances


@timefunc
def cpu_liverpool(**kwargs):
    kwargs["edges"] = kwargs["edges"].to_pandas()
    kwargs["nodes"] = kwargs["nodes"].to_pandas()
    kwargs["postcodes"] = kwargs["postcodes"].to_pandas()
    routing = CPURouting(**kwargs)
    routing.fit()

    postcodes = kwargs.get("postcodes")
    distances = routing.distances.join(postcodes.set_index("node_id")).reset_index()
    return distances


if __name__ == "__main__":
    gpu = gpu_liverpool(**get_data())
    cpu = cpu_liverpool(**get_data())

    gpu.to_pandas().plot(x="easting", y="northing", c="distance", kind="scatter")

    plt.show()
