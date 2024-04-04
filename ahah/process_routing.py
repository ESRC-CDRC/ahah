import cudf
from cuml.neighbors.nearest_neighbors import NearestNeighbors

from ahah.common.logger import logger
from ahah.common.utils import (
    Config,
    clean_bluespace,
    clean_dentists,
    clean_gpp,
    clean_hospitals,
    clean_pharmacies,
    clean_postcodes,
)


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
        .set_index("node_id")
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
    return (
        poi_nn.merge(buffers, on="node_id", how="left")
        .dropna()
        .drop_duplicates("node_id")
    )


if __name__ == "__main__":
    logger.info("Starting routing data processing...")
    logger.debug("Reading and cleaning data...")

    nodes: cudf.DataFrame = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    pcs: cudf.DataFrame = clean_postcodes(
        path=Config.RAW_DATA / "onspd" / "ONSPD_FEB_2024.csv", current=True
    )

    all_pcs: cudf.DataFrame = clean_postcodes(
        path=Config.RAW_DATA / "onspd" / "ONSPD_FEB_2024.csv", current=False
    ).drop("lsoa11", axis=1)
    all_pcs.reset_index().to_parquet(Config.PROCESSED_DATA / "all_pcs.parquet")

    gpp: cudf.DataFrame = clean_gpp(
        england=Config.RAW_DATA / "nhs" / "epraccur_nov2023.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "gpp_jan2024.csv",
        postcodes=all_pcs,
    )
    hospitals: cudf.DataFrame = clean_hospitals(
        england=Config.RAW_DATA / "nhs" / "ets_nov2023.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "hospitals_feb2024.csv",
        postcodes=all_pcs,
    )
    dentists: cudf.DataFrame = clean_dentists(
        england=Config.RAW_DATA / "nhs" / "egdpprac_nov2023.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "dentists_dec2023.csv",
        postcodes=all_pcs,
    )
    df = cudf.read_csv(Config.RAW_DATA / "nhs" / "scotland" / "dentists_dec2023.csv")

    pharmacies: cudf.DataFrame = clean_pharmacies(
        england=Config.RAW_DATA / "nhs" / "edispensary_nov2023.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "pharmacies_nov2023.csv",
        wales=Config.RAW_DATA / "nhs" / "wales" / "pharmacies_nov2023.xls",
        postcodes=all_pcs,
    )

    # bluespace: cudf.DataFrame = clean_bluespace(Config.RAW_DATA / "bluespace")

    logger.debug("Finding nearest node to postcodes...")
    pcs = nearest_nodes(df=pcs.reset_index(), nodes=nodes)
    pcs.to_parquet(Config.PROCESSED_DATA / "postcodes.parquet")

    poi_list = {
        # "gpp": gpp,
        # "hospitals": hospitals,
        "dentists": dentists,
        # "pharmacies": pharmacies,
        # "greenspace": greenspace,
        # "bluespace": bluespace,
    }

    for poi, df in poi_list.items():
        logger.debug(f"Finding nearest node to {poi}...")
        df = nearest_nodes(df.reset_index(drop=True), nodes=nodes)
        df = get_buffers(poi=df, postcodes=pcs, k=10)
        logger.debug(f"Saving {poi}:{Config.PROCESSED_DATA}...")
        df.to_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")
