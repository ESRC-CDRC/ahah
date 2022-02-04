import cudf
from cuml.neighbors.nearest_neighbors import NearestNeighbors

from ahah.common.logger import logger
from ahah.common.utils import (Config, clean_bluespace, clean_dentists,
                               clean_gpp, clean_greenspace_access,
                               clean_hospitals, clean_pharmacies,
                               clean_postcodes)


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
    dist, indices = nbrs.kneighbors(df[["easting", "northing"]])
    df["node_id"] = nodes.iloc[indices]["node_id"].reset_index(drop=True)
    df["dist"] = dist

    return df.loc[df["dist"] < 5000].drop("dist", axis=1)


if __name__ == "__main__":
    logger.info("Starting routing data processing...")
    logger.debug("Reading and cleaning data...")

    nodes = cudf.read_parquet(Config.OS_GRAPH / "nodes.parquet")
    all_pc: cudf.DataFrame = clean_postcodes(
        path=Config.RAW_DATA / "onspd" / "postcodes.csv", current=False
    )
    all_pc.reset_index().to_parquet(Config.PROCESSED_DATA / "all_pc.parquet")
    postcodes: cudf.DataFrame = clean_postcodes(
        path=Config.RAW_DATA / "onspd" / "postcodes.csv", current=True
    )
    gpp: cudf.DataFrame = clean_gpp(
        england=Config.RAW_DATA / "nhs" / "epraccur.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "gpp.csv",
        postcodes=all_pc,
    )
    hospitals: cudf.DataFrame = clean_hospitals(
        england=Config.RAW_DATA / "nhs" / "ets.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "hospitals.csv",
        postcodes=all_pc,
    )
    dentists: cudf.DataFrame = clean_dentists(
        england=Config.RAW_DATA / "nhs" / "egdpprac.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "dentists.csv",
        postcodes=all_pc,
    )
    pharmacies: cudf.DataFrame = clean_pharmacies(
        england=Config.RAW_DATA / "nhs" / "edispensary.csv",
        scotland=Config.RAW_DATA / "nhs" / "scotland" / "pharmacies.csv",
        postcodes=all_pc,
    )
    greenspace: cudf.DataFrame = clean_greenspace_access(
        Config.RAW_DATA / "greenspace/access.shp"
    )
    bluespace: cudf.DataFrame = clean_bluespace(Config.RAW_DATA / "bluespace")

    logger.debug("Finding nearest node to postcodes...")

    postcodes = nearest_nodes(df=postcodes.reset_index(), nodes=nodes)
    postcodes.to_parquet(Config.PROCESSED_DATA / "postcodes.parquet")

    poi_list = {
        "gpp": gpp,
        "hospitals": hospitals,
        "dentists": dentists,
        "pharmacies": pharmacies,
        "greenspace": greenspace,
        "bluespace": bluespace,
    }

    for poi, df in poi_list.items():
        logger.debug(f"Finding nearest node to {poi}...")
        df = nearest_nodes(df.reset_index(drop=True), nodes=nodes)
        logger.debug(f"Saving {poi}:{Config.PROCESSED_DATA}...")
        df.to_parquet(Config.PROCESSED_DATA / f"{poi}.parquet")
