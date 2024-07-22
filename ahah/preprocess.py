import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
import urllib.request
from io import BytesIO
from zipfile import ZipFile

import polars as pl
from ukroutes.oproad.utils import process_oproad

from ahah.common.utils import Config, Paths
from pathlib import Path


def _read_zip_from_url(filename: str) -> BytesIO:
    r = urllib.request.urlopen(Config.NHS_URL + filename).read()
    file = ZipFile(BytesIO(r))
    return file.open(f"{Path(filename).stem}.csv")


def _fetch_scot_records(resource_id: int, limit: int = 100) -> pl.DataFrame:
    initial_url = f"{Config.NHS_SCOT_URL}?resource_id={resource_id}&limit={limit}"
    response = urllib.request.urlopen(initial_url)
    data = response.read().decode()
    data_dict = json.loads(data)
    total_records = data_dict["result"]["total"]
    records = data_dict["result"]["records"]

    offset = limit
    while offset < total_records:
        paginated_url = f"{Config.NHS_SCOT_URL}?resource_id={resource_id}&limit={limit}&offset={offset}"
        response = urllib.request.urlopen(paginated_url)
        data = response.read().decode()
        data_dict = json.loads(data)
        records.extend(data_dict["result"]["records"])
        offset += limit
    return pl.DataFrame(records)


def process_postcodes():
    (
        pl.read_csv(
            Paths.RAW / "onspd" / "ONSPD_FEB_2024.csv",
            columns=["PCD", "OSEAST1M", "OSNRTH1M", "DOTERM", "CTRY"],
        )
        .rename({"PCD": "postcode", "OSEAST1M": "easting", "OSNRTH1M": "northing"})
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .filter(
            (pl.col("DOTERM").is_null())
            & (pl.col("CTRY").is_in(["N92000002", "L93000001", "M83000003"]).not_())
        )
        .drop(["DOTERM", "CTRY"])
        .drop_nulls()
        .write_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")
    )


def process_hospitals(postcodes):
    eng_csv_path = Paths.RAW / "nhs" / "hospitals_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_FILES["hospitals"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10", "column_12"])
            .rename({"column_1": "code", "column_10": "postcode", "column_12": "close"})
            .filter(pl.col("close") != "")
            .drop("close")
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "hospitals_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["hospitals"])
            .select(["HospitalCode", "Postcode"])
            .rename({"HospitalCode": "code", "Postcode": "postcode"})
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "hospitals.parquet")
    )


def process_gpp(postcodes):
    eng_csv_path = Paths.RAW / "nhs" / "gpp_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_FILES["gpp"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "gpp_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["gpp"])
            .select(["PracticeCode", "Postcode"])
            .rename({"PracticeCode": "code", "Postcode": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "gpp.parquet")
    )


def process_dentists(postcodes):
    eng_csv_path = Paths.RAW / "nhs" / "dentists_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_FILES["dentists"])
        (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "dentists_scotland.csv"
    if not scot_csv_path.exists():
        (
            _fetch_scot_records(Config.NHS_SCOT_FILES["dentists"])
            .select(["Dental_Practice_Code", "pc7"])
            .rename({"Dental_Practice_Code": "code", "pc7": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)
    (
        pl.concat([eng, scot])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "dentists.parquet")
    )


def process_pharmacies(postcodes):
    eng_csv_path = Paths.RAW / "nhs" / "pharmacies_england.csv"
    if not eng_csv_path.exists():
        eng_csv = _read_zip_from_url(Config.NHS_FILES["pharmacies"])
        eng = (
            pl.read_csv(eng_csv, has_header=False)
            .select(["column_1", "column_10"])
            .rename({"column_1": "code", "column_10": "postcode"})
            .write_csv(eng_csv_path)
        )
    eng = pl.read_csv(eng_csv_path)

    scot_csv_path = Paths.RAW / "nhs" / "pharmacies_scotland.csv"
    if not scot_csv_path.exists():
        (
            pl.read_csv(Config.NHS_SCOT_FILES["pharmacies"])
            .select(["DispCode", "DispLocationPostcode"])
            .rename({"DispCode": "code", "DispLocationPostcode": "postcode"})
            .with_columns(("c" + pl.col("code").cast(pl.String)).alias("code"))
            .write_csv(scot_csv_path)
        )
    scot = pl.read_csv(scot_csv_path)

    wales_csv_path = Paths.RAW / "nhs" / "pharmacies_wales.csv"
    if not wales_csv_path.exists():
        (
            pl.read_excel(Config.NHS_WALES_URL + Config.NHS_WALES_FILES["pharmacies"])
            .select(["Account Number", "Post Code"])
            .rename({"Account Number": "code", "Post Code": "postcode"})
            .write_csv(wales_csv_path)
        )
    wales = pl.read_csv(wales_csv_path)

    (
        pl.concat([eng, scot, wales])
        .with_columns(pl.col("postcode").str.replace(" ", ""))
        .join(postcodes, on="postcode")
        .select(["code", "easting", "northing"])
        .write_parquet(Paths.PROCESSED / "pharmacies.parquet")
    )


def process_bluespace():
    bluespace = gpd.read_parquet(Paths.RAW / "osm" / "gb-water.parquet")
    coast = (
        gpd.read_parquet(Paths.RAW / "osm" / "gb-coast.parquet")
        .to_crs(27700)
        .get_coordinates()
        .round(-3)
    )
    bs = bluespace[
        (bluespace.geometry.apply(lambda x: isinstance(x, (MultiPolygon, Polygon))))
    ].to_crs(27700)
    bs = bs[bs.area > 10_000].get_coordinates().round(-3)
    bs = (
        pd.concat([coast, bs])
        .drop_duplicates()
        .rename(columns={"x": "easting", "y": "northing"})
    )
    bs.to_parquet(Paths.PROCESSED / "bluespace.parquet", index=False)


def main():
    process_postcodes()
    postcodes = pl.read_parquet(Paths.PROCESSED / "onspd" / "postcodes.parquet")

    process_hospitals(postcodes)
    process_gpp(postcodes)
    process_dentists(postcodes)
    process_pharmacies(postcodes)
    process_bluespace()

    _ = process_oproad(outdir=Paths.PROCESSED / "oproad")


if __name__ == "__main__":
    main()
