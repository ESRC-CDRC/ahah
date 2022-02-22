from pathlib import Path
from typing import List, Union

import cudf
import geopandas as gpd
import pandas as pd

DataFrame = Union[pd.DataFrame, cudf.DataFrame]


class Config:
    """Misc constants required throughout"""

    DATA_PATH = Path("/scratch/data")
    RAW_DATA = DATA_PATH / "raw"
    PROCESSED_DATA = DATA_PATH / "processed"
    OUT_DATA = DATA_PATH / "out"
    OS_GRAPH = PROCESSED_DATA / "osm"
    POI_LIST = [
        "fastfood",
        "gambling",
        "leisure",
        "offlicences",
        "pubs",
        "tobacconists",
    ]


def combine_lsoa(eng, scot, wales):
    eng = gpd.read_file(eng)[["code", "name", "geometry"]].rename(
        columns={"code": "lsoa11"}
    )
    scot = gpd.read_file(scot)[["DataZone", "Name", "geometry"]].rename(
        columns={"DataZone": "lsoa11", "Name": "name"}
    )
    wales = gpd.read_file(wales)[["LSOA11Code", "lsoa11name", "geometry"]].rename(
        columns={"LSOA11Code": "lsoa11", "lsoa11name": "name"}
    )
    return eng.append(scot).append(wales)


def fix_postcodes(df: DataFrame) -> DataFrame:
    """
    Ensure all postcodes follow correct spacing format

    Parameters
    ----------
    series : DataFrame
        Df with Series of postcodes as strings

    Returns
    -------
    DataFrame:
        Df with correctly formatted series of postcodes
    """
    df["postcode"] = df["postcode"].str.replace(" ", "")
    df["postcode"] = df["postcode"].str[:-3] + " " + df["postcode"].str[-3:]
    return df


def find_partial_pc(df, postcodes):
    """
    Find postcodes that have partially correct first characters.

    Splits postcodes into two and uses first half to match to likely nearest
    correct postcode location if the postcode cannot be found in whole postcode
    dataset.

    Parameters
    ----------
    poi : Union[pd.DataFrame, cudf.DataFrame]
        POI dataframe containing series of postcodes
    poi_pc : Union[pd.DataFrame, cudf.DataFrame]
        POI dataframe with df postcodes
    postcodes : Union[pd.DataFrame, cudf.DataFrame]
        Dataframe of all postcodes
    Returns
    -------
    Union[pd.DataFrame, cudf.DataFrame]:
        POI dataframe with inferred postcodes
    """
    remove_pcs = "|".join(["GY", "JE", "IM", "BF"])

    df = df[~df.index.str.contains(remove_pcs).values]
    missing = df[df["easting"].isna()].reset_index()
    postcodes = postcodes.reset_index()

    missing["partial"] = missing["postcode"].str.split(" ", expand=True)[0]
    postcodes["partial"] = postcodes["postcode"].str.split(" ", expand=True)[0]

    missing = missing.set_index("partial").drop(
        ["postcode", "easting", "northing", "node_id"], axis=1
    )
    postcodes = postcodes.set_index("partial")

    missing = (
        missing.join(postcodes)
        .reset_index()
        .drop_duplicates("partial")
        .drop("partial", axis=1)
        .set_index("postcode")
        .dropna()
    )

    return df.dropna().append(missing)


def clean_retail(path: Path, postcodes: cudf.DataFrame) -> cudf.DataFrame:
    retail = cudf.read_csv(path)
    retail = retail[["PremiseId", "Category", "Subcategory", "PostCode"]]

    retail = (
        retail.rename(
            columns={
                "PremiseId": "id",
                "PostCode": "postcode",
                "Category": "category",
                "Subcategory": "subcategory",
            }
        )
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
    )
    return retail.reset_index()


def subset_retail(
    retail: cudf.DataFrame,
    types: List[str],
    column: str = "subcategory",
) -> cudf.DataFrame:
    return retail[retail[column].isin(types)]


def clean_fast_food(retail: cudf.DataFrame):
    types = [
        "Chinese Fast Food Takeaway",
        "Fast Food Delivery",
        "Fish & Chip Shops",
        "Indian Takeaway",
        "Pizza Takeaway",
        "Sandwich Delivery Service",
        "Take Away Food Shops",
    ]
    return subset_retail(retail, types)


def clean_gambling(retail: cudf.DataFrame):
    types = ["Casino Clubs", "Bookmakers"]
    return subset_retail(retail, types)


def clean_offlicences(retail: cudf.DataFrame):
    types = ["Off Licences"]
    return subset_retail(retail, types, column="category")


def clean_pubs(retail: cudf.DataFrame):
    types = ["Night Clubs", "Bars", "Public Houses & Inns"]
    return subset_retail(retail, types)


def clean_tobacconists(retail: cudf.DataFrame):
    types = ["Vaping Stores and Tobacconists"]
    return subset_retail(retail, types)


def clean_leisure(retail: cudf.DataFrame):
    types = ["Leisure Centres & Swimming Baths", "Health Clubs"]
    return subset_retail(retail, types)
