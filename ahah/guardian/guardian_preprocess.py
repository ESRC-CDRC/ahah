import geopandas as gpd
import polars as pl
from pyproj import Transformer

from ahah.common.utils import Paths

transformer = Transformer.from_crs("epsg:4326", "epsg:27700")


def process_pubs():
    (
        pl.read_csv(
            "./data/raw/guardian/pubs.csv",
            infer_schema_length=100_000,
            columns=["FHRSID", "lat", "long"],
        )
        .drop_nulls()
        .with_columns(
            pl.struct(pl.col("lat"), pl.col("long"))
            .map_elements(
                lambda row: transformer.transform(row["lat"], row["long"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["FHRSID", "easting", "northing"])
        .write_parquet("./data/processed/guardian/pubs.parquet")
    )


def process_cinemas():
    (
        pl.read_csv(
            "./data/raw/guardian/cinemas.csv", columns=["full_address", "lat", "long"]
        )
        .with_columns(
            pl.struct(pl.col("lat"), pl.col("long"))
            .map_elements(
                lambda row: transformer.transform(row["lat"], row["long"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["full_address", "easting", "northing"])
        .write_parquet("./data/processed/guardian/cinemas.parquet")
    )


def process_libraries():
    (
        pl.read_csv(
            "./data/raw/guardian/libraries.csv", columns=["id", "Latitude", "Longitude"]
        )
        .with_columns(
            pl.struct(pl.col("Latitude"), pl.col("Longitude"))
            .map_elements(
                lambda row: transformer.transform(row["Latitude"], row["Longitude"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["id", "easting", "northing"])
        .write_parquet("./data/processed/guardian/libraries.parquet")
    )


def process_museums():
    (
        pl.read_csv(
            "./data/raw/guardian/museums_galleries.csv",
            columns=["id", "latitude", "longitude"],
        )
        .with_columns(
            pl.struct(pl.col("latitude"), pl.col("longitude"))
            .map_elements(
                lambda row: transformer.transform(row["latitude"], row["longitude"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("coords")
        )
        .with_columns(
            pl.col("coords").list[0].alias("easting"),
            pl.col("coords").list[1].alias("northing"),
        )
        .select(["id", "easting", "northing"])
        .write_parquet("./data/processed/guardian/museums.parquet")
    )


def process_greenspace():
    gs = gpd.read_file(Paths.RAW / "oproad" / "opgrsp_gb.gpkg", layer="access_point")
    gs["easting"], gs["northing"] = gs.geometry.x, gs.geometry.y
    gs = gs.round(-1).drop_duplicates(subset=["easting", "northing"])
    pl.from_pandas(gs[["id", "easting", "northing"]]).write_parquet(
        Paths.PROCESSED / "guardian" / "greenspace.parquet"
    )


if __name__ == "__main__":
    process_pubs()
    process_cinemas()
    process_libraries()
    process_museums()
    process_greenspace()
