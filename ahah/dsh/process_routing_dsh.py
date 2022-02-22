import cudf
from ahah.common.utils_dsh import (
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
    df = df.dropna(subset=["easting", "northing"])
    nbrs = NearestNeighbors(n_neighbors=1, output_type="cudf", algorithm="brute").fit(
        nodes[["easting", "northing"]]
    )
    _, indices = nbrs.kneighbors(df[["easting", "northing"]])

    df["node_id"] = nodes.iloc[indices]["node_id"].reset_index(drop=True)
    return df


def get_buffers(
    poi: cudf.DataFrame,
    postcodes: cudf.DataFrame,
    k: int,
) -> cudf.DataFrame:
    """
    Estimate buffer sizes required to capture each necessary road node
    Calculates k nearest neighbours for each POI to each road node. Finds
    each node that is considered a neighbour to a poi `k*len(poi)`. Buffers
    are taken as the distance to the further neighbour and all nodes associated with
    each POI are saved.
    Parameters
    ----------
    poi : cudf.DataFrame
        Dataframe of all POIs
    postcodes : cudf.DataFrame
        Dataframe of postcodes
    k : int
        Number of neigbours to use
    Returns
    -------
    cudf.DataFrame:
        POI dataframe including buffer and column with list of nodes
    """
    nbrs = NearestNeighbors(n_neighbors=k, output_type="cudf", algorithm="brute").fit(
        poi[["easting", "northing"]]
    )
    distances, indices = nbrs.kneighbors(postcodes[["easting", "northing"]])

    poi_nn = (
        postcodes.join(indices)[["node_id"] + indices.columns.tolist()]
        .set_index("node_id")  # type:ignore
        .stack()
        .rename("poi_idx")
        .reset_index()
        .rename(columns={"level_0": "pc_node"})
        .drop("level_1", axis=1)
        .groupby("poi_idx")
        .agg(list)
        .join(poi, how="right")
    )

    # retain only unique postcode ids
    poi_nn["pc_node"] = (
        poi_nn["pc_node"]
        .to_pandas()
        .apply(lambda row: list(set(row)) if row is not None else row)
    )

    distances = distances.stack().rename("dist").reset_index().drop("level_1", axis=1)
    indices = indices.stack().rename("ind").reset_index().drop("level_1", axis=1)

    poi_nodes = (
        poi_nn[["node_id"]]
        .iloc[indices["ind"].values]["node_id"]
        .reset_index(drop=True)
    )
    buffers = cudf.DataFrame({"node_id": poi_nodes, "buffer": distances["dist"].values})
    buffers = buffers.sort_values("buffer", ascending=False).drop_duplicates("node_id")
    buffers["buffer"] = buffers["buffer"].astype("int")

    # this will drop rows that did not appear in the KNN i.e unneeded poi
    return poi_nn.merge(buffers, on="node_id", how="left").dropna()


if __name__ == "__main__":
    print("Starting routing data processing...")
    print("Reading and cleaning data...")

    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")

    pcs: cudf.DataFrame = cudf.read_parquet(
        Config.PROCESSED_DATA / "postcodes.parquet"
    )
    retail: cudf.DataFrame = clean_retail(
        path=Config.RAW_DATA / "LDC_Secure_Snapshot_2020_01.csv", postcodes=pcs
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
        df = nearest_nodes(df.reset_index(drop=True), nodes=nodes)
        df = get_buffers(poi=df, postcodes=pcs.reset_index(), k=5)
        print(f"Saving {poi}:{Config.PROCESSED_DATA}...")
        df.to_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")
