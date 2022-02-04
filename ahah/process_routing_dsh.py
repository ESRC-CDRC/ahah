import cudf
from ahah.common.utils import (
    Config,
    clean_fast_food,
    clean_gambling,
    clean_leisure,
    clean_offlicences,
    clean_pubs,
    clean_retail,
    clean_tobacconists,
)
from cuml.neighbors.nearest_neighbors import NearestNeighbors
from pathlib import Path


def nearest_nodes(df: cudf.DataFrame, nodes: cudf.DataFrame) -> cudf.DataFrame:
    """
    Find nearest road node to point of interest

    Uses `cuml` nearest neighbours for GPU accelerated nearest points.
    It is assumed that all points use a planar coordinate system like BNG.

    Parameters
    ----------
    df : cudf.DataFrame
        POI df containing coordinate information
    nodes : cudf.DataFrame
        Road nodes with coordinate information

    Returns
    -------
    cudf.DataFrame:
        Road nodes that are nearest neighbour to some POI
    """
    nbrs = NearestNeighbors(n_neighbors=1, output_type="cudf", algorithm="brute").fit(
        nodes[["easting", "northing"]]
    )
    _, indices = nbrs.kneighbors(df[["easting", "northing"]])

    df["node_id"] = nodes.iloc[indices]["node_id"].reset_index(drop=True)
    return df


if __name__ == "__main__":
    print("Starting routing data processing...")
    print("Reading and cleaning data...")

    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")

    postcodes: cudf.DataFrame = cudf.read_parquet("data/processed/postcodes.parquet")
    all_pc: cudf.DataFrame = cudf.read_parquet(
        "data/processed/all_pc.parquet"
    ).set_index("postcode")

    retail: cudf.DataFrame = clean_retail(
        path=Path("data/raw/LDC_Secure_Snapshot_2020_01.csv"), postcodes=all_pc
    )
    fast_food: cudf.DataFrame = clean_fast_food(retail=retail)
    gambling: cudf.DataFrame = clean_gambling(retail=retail)
    offlicences: cudf.DataFrame = clean_offlicences(retail=retail)
    pubs: cudf.DataFrame = clean_pubs(retail=retail)
    tobacconists: cudf.DataFrame = clean_tobacconists(retail=retail)
    leisure: cudf.DataFrame = clean_leisure(retail=retail)

    print("Finding nearest node to postcodes...")

    postcodes = nearest_nodes(df=postcodes.reset_index(), nodes=nodes)

    poi_list = {
        "fast_food": fast_food,
        "gambling": gambling,
        "offlicences": offlicences,
        "pubs": pubs,
        "tobacconists": tobacconists,
        "leisure": leisure,
    }
    assert len(poi_list) == len(Config.POI_LIST)

    for poi, df in poi_list.items():
        print(f"Finding nearest node to {poi}...")
        df = nearest_nodes(df.reset_index(drop=True), nodes=nodes)
        print(f"Saving {poi}:{Config.PROCESSED_DATA}...")
        df.to_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")
