from ahah.common.utils_dsh import Config
import pandas as pd
from ahah.common.utils_dsh import (
    clean_fast_food,
    clean_gambling,
    clean_leisure,
    clean_offlicences,
    clean_pubs,
    clean_retail,
    clean_tobacconists,
)
from ahah.process_routing_dsh import add_to_graph, add_topk
from ahah.routing_dsh import Routing
import cudf

nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
edges = cudf.read_parquet(Config.OS_GRAPH / "edges.parquet")

postcodes: cudf.DataFrame = cudf.read_parquet(
    Config.PROCESSED_DATA / "postcodes.parquet"
).set_index("postcode")

postcodes = pd.read_csv(
    Config.RAW_DATA / "onspd" / "ONSPD_FEB_2024.csv",
    usecols=["PCD", "OSEAST1M", "OSNRTH1M", "DOTERM", "CTRY"],
)
postcodes = (
    postcodes[
        (postcodes["DOTERM"].isnull())
        & (~postcodes["CTRY"].isin(["N92000002", "L93000001", "M83000003"]))
    ]
    .drop(columns=["DOTERM", "CTRY"])
    .rename({"PCD": "postcode", "OSEAST1M": "easting", "OSNRTH1M": "northing"}, axis=1)
    .dropna()
    .reset_index(drop=True)
)

postcodes, nodes, edges = add_to_graph(postcodes, nodes, edges, 1)

retail: cudf.DataFrame = clean_retail(
    path=Config.RAW_DATA / "LDC_Secure_Snapshot_2023_07.csv", postcodes=postcodes
)
fast_food: cudf.DataFrame = clean_fast_food(retail=retail)
gambling: cudf.DataFrame = clean_gambling(retail=retail)
offlicences: cudf.DataFrame = clean_offlicences(retail=retail)
pubs: cudf.DataFrame = clean_pubs(retail=retail)
tobacconists: cudf.DataFrame = clean_tobacconists(retail=retail)
leisure: cudf.DataFrame = clean_leisure(retail=retail)

poi_list = {
    "fastfood": fast_food,
    "gambling": gambling,
    "offlicences": offlicences,
    "pubs": pubs,
    "tobacconists": tobacconists,
    "leisure": leisure,
}
assert len(poi_list) == len(Config.POI_LIST)

for poi, df in poi_list.items():
    print(f"Finding nearest node to {poi}...")
    df, nodes, edges = add_to_graph(df, nodes, edges, 1)
    df = add_topk(df, postcodes, 10)
    print(f"Saving {poi}:{Config.PROCESSED_DATA}...")
    df.to_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")

    # run the routing class
    routing = Routing(
        edges=edges,
        nodes=nodes,
        inputs=df,
        outputs=postcodes,
        weights="time_weighted",
        min_buffer=5000,
        max_buffer=500_000,
    )
    routing.fit()

    # join distances to postcodes
    distances = (
        routing.distances.set_index("vertex")
        .join(cudf.from_pandas(postcodes).set_index("node_id"), how="right")
        .reset_index()
        .to_pandas()
    )
    distances.to_parquet(Config.PROCESSED_DATA / f"{poi}_distances.parquet")
