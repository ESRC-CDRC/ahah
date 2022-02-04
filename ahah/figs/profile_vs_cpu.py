import time
from typing import Union

import cudf
import pandas as pd

from ahah.common.utils import Config
from ahah.process_routing import nearest_nodes
from ahah.routing import Routing
from ahah.routing_cpu import CPURouting


def get_data(pc: Union["str", list], use_gpu: bool):
    edges = cudf.read_parquet(Config.OS_GRAPH / "edges.parquet")
    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    postcodes = cudf.read_parquet(Config.PROCESSED_DATA / "postcodes.parquet")

    if isinstance(pc, str):
        postcodes = postcodes[postcodes["postcode"].str.match(pc)]
    elif isinstance(pc, list):
        postcodes = postcodes[postcodes["postcode"].isin(pc)]

    gpp = cudf.read_parquet(Config.PROCESSED_DATA / "gpp.parquet")
    gpp = gpp[gpp["postcode"].isin(postcodes["postcode"])]
    gpp = nearest_nodes(gpp.reset_index(drop=True), nodes=nodes)

    return {
        "name": "gpp",
        "edges": edges if use_gpu else edges.to_pandas(),
        "nodes": nodes if use_gpu else nodes.to_pandas(),
        "postcodes": postcodes if use_gpu else postcodes.to_pandas(),
        "pois": gpp.to_pandas(),
        "buffer": 50_000,
    }


def cpu_liverpool(**kwargs):
    routing = CPURouting(**kwargs)
    t1 = time.time()
    routing.fit()
    t2 = time.time()

    postcodes = kwargs.get("postcodes")
    return routing.distances.join(postcodes.set_index("node_id")), t2 - t1


def gpu_liverpool(**kwargs):
    routing = Routing(**kwargs)
    t1 = time.time()
    routing.fit()
    t2 = time.time()

    postcodes = kwargs.get("postcodes")
    return routing.distances.join(postcodes.set_index("node_id")), t2 - t1


PC = r"^L\d.*"
PC = r"^SK17.*"
PC = r"^E\d\s.*"
hp_pcs = (
    pd.read_csv(
        "https://www.doogal.co.uk/" "AdministrativeAreasCSV.ashx?district=E07000037"
    )
    .loc[lambda row: row["In Use?"] == "Yes", "Postcode"]
    .tolist()
)

cpu, cpu_time = cpu_liverpool(**get_data(pc=PC, use_gpu=False))
gpu, gpu_time = gpu_liverpool(**get_data(pc=PC, use_gpu=True))

gpu_plot = gpu.to_pandas().plot(x="easting", y="northing", c="distance", kind="scatter")
