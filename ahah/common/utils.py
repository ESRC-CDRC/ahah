import os
from pathlib import Path
from typing import Union

import cudf
import dask_geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon
from tqdm import tqdm

from ahah.common.logger import logger

DataFrame = Union[pd.DataFrame, cudf.DataFrame]


class Config:
    """Misc constants required throughout"""

    DATA_PATH = Path(os.environ["PROJECT_ROOT"]) / "data"
    RAW_DATA = DATA_PATH / "raw"
    PROCESSED_DATA = DATA_PATH / "processed"
    OUT_DATA = DATA_PATH / "out"
    OS_GRAPH = PROCESSED_DATA / "osm"
    HW_DATA = RAW_DATA / "os_highways" / "oproad_gb.gpkg"

    POI_LIST = [
        "gpp",
        "dentists",
        "pharmacies",
        "hospitals",
        "greenspace",
        "bluespace",
    ]

    NODE_COLS = ["node_id", "easting", "northing"]
    EDGE_COLS = ["source", "target", "time_weighted", "length"]

    # WARN:In final version find out exact dates and update Scotland to newest.
    # https://digital.nhs.uk/services/organisation-data-service/data-downloads
    NHS_URL = "https://files.digital.nhs.uk/assets/ods/current/"
    NHS_FILES = {
        # 25 Feburary 2022 - Checked 22 March 2022
        "gpp": "epraccur.zip",
        # ""
        "dentists": "egdpprac.zip",
        # ""
        "pharmacies": "edispensary.zip",
        # ""
        "hospitals": "ets.zip",
    }
    NHS_SCOT_URL = "https://www.opendata.nhs.scot/dataset/"
    NHS_SCOT_FILES = {
        # GP Practices and List sizes Jan 2022
        "gpp": "f23655c3-6e23-4103-a511-a80d998adb90/resource"
        "/a794d603-95ab-4309-8c92-b48970478c14/download"
        "/practice_contactdetails_jan2022.csv",
        # Dental Practices June 2021
        "dentists": "2f218ba7-6695-4b22-867d-41383ae36de7/resource"
        "/12bf4b02-15e6-41d0-9ae0-18663b463833/download"
        "/nhs-dental-practices-and-nhs-dental-registrations-as-at-30th-june-2021.csv",
        # Current NHS Hospitals in Scotland 16th December 2021
        "hospitals": "cbd1802e-0e04-4282-88eb-d7bdcfb120f0/resource"
        "/c698f450-eeed-41a0-88f7-c1e40a568acc/download"
        "/current-hospital_flagged20211216.csv",
        # Dispenser Details October 2021
        "pharmacies": "a30fde16-1226-49b3-b13d-eb90e39c2058/resource"
        "/9f9db0c9-8b5a-4813-b586-7e0084bbf9b0/download"
        "/dispenser_contactdetails_oct2021.csv",
    }
    NHS_WALES_URL = (
        "https://nwssp.nhs.wales/ourservices/"
        "primary-care-services/primary-care-services-documents/"
    )
    NHS_WALES_FILES = {
        "pharmacy": (
            "pharmacy-practice-dispensing-data-docs"
            "/dispensing-data-report-november-2021"
        )
    }


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
        ["postcode", "easting", "northing"], axis=1
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


def clean_postcodes(path: Path, current: bool) -> cudf.DataFrame:
    logger.info("Cleaning postcodes...")

    dtypes = {
        "pcd": "str",
        "lsoa11": "str",
        "oseast1m": "int",
        "osnrth1m": "int",
        "doterm": "int",
        "ctry": "str",
    }
    column_names = {
        "pcd": "postcode",
        "oseast1m": "easting",
        "osnrth1m": "northing",
    }

    postcodes = (
        cudf.read_csv(
            path,
            usecols=[*dtypes],
            dtype=dtypes,
        )
        .rename(columns=column_names)
        .set_index("ctry")
        .loc[["E92000001", "S92000003", "W92000004"]]
        .pipe(fix_postcodes)
        .dropna(subset=["northing", "easting"])
        .set_index("postcode")
        .drop_duplicates()
    )

    if not current:
        return postcodes.drop("doterm", axis=1)

    # 2 scottish LSOA have no current postcodes - so use nearby ones
    pc_current = postcodes[postcodes["doterm"].isnull()]
    missing = postcodes.copy()
    missing.loc["G21 4QJ", "lsoa11"] = "S01010206"
    missing.loc[["G21 1NL", "G21 1RR"], "lsoa11"] = "S01010226"

    return (
        pc_current.append(postcodes[postcodes["lsoa11"] == "S01011827"])
        .append(missing.loc[["G21 4QJ", "G21 1NL", "G21 1RR"]])
        .drop("doterm", axis=1)
    )


def clean_dentists(
    england: Path, scotland: Path, postcodes: cudf.DataFrame
) -> cudf.DataFrame:
    logger.info("Cleaning dentists...")

    edent = (
        cudf.read_csv(england, usecols=[0, 9], header=None)
        .rename(columns={"0": "dentist", "9": "postcode"})
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    sdent = (
        cudf.read_csv(scotland)
        .rename(
            columns={
                "﻿DentalPracticeCode": "dentist",
                "Postcode": "postcode",
            }
        )[["dentist", "postcode"]]
        .astype(str)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    return edent.append(sdent).reset_index()


def clean_gpp(
    england: Path, scotland: Path, postcodes: cudf.DataFrame
) -> cudf.DataFrame:
    logger.info("Cleaning gpp...")

    egpp = (
        cudf.read_csv(england, usecols=[0, 9, 11], header=None)
        .rename(columns={"0": "gpp", "9": "postcode", "11": "close"})
        .pipe(lambda x: x[x["close"].isna()])
        .drop("close", axis=1)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    sgpp = (
        cudf.read_csv(scotland, usecols=["PracticeCode", "Postcode"])
        .rename(columns={"PracticeCode": "gpp", "Postcode": "postcode"})
        .astype(str)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    return egpp.append(sgpp).reset_index()


def clean_pharmacies(
    england: Path, scotland: Path, wales: Path, postcodes: cudf.DataFrame
) -> cudf.DataFrame:
    logger.info("Cleaning pharmacies...")

    epharm = (
        cudf.read_csv(england, header=None, usecols=[0, 9, 11])
        .rename(columns={"0": "pharmacy", "9": "postcode", "11": "close"})
        .pipe(lambda x: x[x["close"].isnull()])
        .drop("close", axis=1)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    spharm = (
        cudf.read_csv(scotland, usecols=[0, 6], sep="\t")
        .rename(columns={"DispenserCode": "pharmacy", "Postcode": "postcode"})
        .astype(str)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    wpharm = (
        cudf.from_pandas(pd.read_excel(wales, usecols=["Account Number", "Post Code"]))
        .rename(columns={"Account Number": "pharmacy", "Post Code": "postcode"})
        .astype(str)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )
    return epharm.append(spharm).append(wpharm).reset_index()


def clean_hospitals(
    england: Path, scotland: Path, postcodes: cudf.DataFrame
) -> cudf.DataFrame:
    ehos = (
        cudf.read_csv(england, usecols=[0, 9, 11], header=None)
        .rename(columns={"0": "hospital", "9": "postcode", "11": "close"})
        .pipe(lambda x: x[x["close"].isnull()])
        .drop("close", axis=1)
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )

    shos = (
        cudf.read_csv(scotland, usecols=["Location", "Postcode"])
        .rename(columns={"Location": "hospital", "Postcode": "postcode"})
        .pipe(fix_postcodes)
        .set_index("postcode")
        .join(postcodes)
        .pipe(find_partial_pc, postcodes)
    )
    return ehos.append(shos).reset_index()


def clean_air(path: Path, col: "str"):
    logger.info(f"Cleaning air: {path}:{col}")
    air: pd.DataFrame = pd.read_csv(path, skiprows=5, header=0)
    air = air[air[col] != "MISSING"]
    air[col] = air[col].astype(float)
    return air


# def clean_greenspace_access(path: Path) -> cudf.DataFrame:
#     logger.info("Cleaning greenspace access...")
#     greenspace = gpd.read_file(path)
#     greenspace["easting"], greenspace["northing"] = (
#         greenspace.geometry.x,
#         greenspace.geometry.y,
#     )
#     return cudf.DataFrame(greenspace.drop("geometry", axis=1)).loc[
#         :, ["id", "easting", "northing"]
#     ]


def clean_greenspace_access(england: Path, scotland: Path, wales: Path):
    england = gpd.read_file(england, crs=4326)
    scotland = gpd.read_file(scotland, crs=4326)
    wales = gpd.read_file(wales, crs=4326)
    greenspace = dask_geopandas.from_geopandas(
        wales.append(england).append(scotland), npartitions=os.cpu_count()
    )

    greenspace = greenspace[greenspace["fclass"] == "path"].to_crs(27700).compute()

    greenspace = (
        greenspace.geometry.boundary.apply(lambda x: x.geoms).explode().dropna()
    )
    greenspace = gpd.GeoDataFrame(greenspace.rename("geometry"), geometry="geometry")
    greenspace["easting"] = greenspace.geometry.x
    greenspace["northing"] = greenspace.geometry.y

    greenspace["easting"] = greenspace["easting"].round(-2).astype(int)
    greenspace["northing"] = greenspace["northing"].round(-2).astype(int)

    return cudf.from_pandas(greenspace[["easting", "northing"]]).drop_duplicates()


def single_parametric_interpolate(obj_x_loc, obj_y_loc, num_pts: int):
    # https://stackoverflow.com/questions/42023522/random-sampling-of-points-along-a-polygon-boundary
    if obj_x_loc is None or obj_y_loc is None:
        return None

    num_coords = len(obj_x_loc)

    vi = [
        [
            obj_x_loc[(i + 1) % num_coords] - obj_x_loc[i],
            obj_y_loc[(i + 1) % num_coords] - obj_y_loc[i],
        ]
        for i in range(num_coords)
    ]
    si = [np.linalg.norm(v) for v in vi]
    di = np.linspace(0, sum(si), num_pts, endpoint=False)
    new_points = []
    for d in di:
        for i, s in enumerate(si):
            if d > s:
                d -= s
            else:
                break
        lnth = d / s
        new_points.append(
            [int(obj_x_loc[i] + lnth * vi[i][0]), int(obj_y_loc[i] + lnth * vi[i][1])]
        )
    return new_points


def catch_exterior(row):
    return row.exterior.coords.xy if isinstance(row, Polygon) else None


def clean_bluespace(data_dir: Path) -> cudf.DataFrame:
    logger.info("Cleaning bluespace...")
    bluespace = gpd.GeoDataFrame(
        pd.concat(
            [gpd.read_file(shp) for shp in tqdm(list(data_dir.glob("*.shp")))],
            ignore_index=True,
        )
    )

    high_water = bluespace[bluespace["CLASSIFICA"] == "High Water Mark"]
    bluespace = bluespace[(bluespace.geometry.area > 10_000)]
    bluespace.geometry = bluespace.geometry.simplify(25)
    high_water.geometry = high_water.geometry.simplify(25)

    tqdm.pandas()
    high_water = high_water.progress_apply(lambda x: x.geometry.coords.xy, axis=1)
    bluespace = bluespace.progress_apply(
        lambda x: catch_exterior(x.geometry), axis=1
    ).dropna()
    bluespace = bluespace.append(high_water)
    bluespace = pd.DataFrame(bluespace.tolist())

    tqdm.pandas()
    bluespace = cudf.DataFrame(
        bluespace.progress_apply(
            lambda row: single_parametric_interpolate(row[0], row[1], num_pts=10),
            axis=1,
        )
        .explode()
        .dropna()
        .tolist(),
        columns=["easting", "northing"],
    )

    bluespace["easting"] = bluespace["easting"].round(-2)
    bluespace["northing"] = bluespace["northing"].round(-2)

    return bluespace.drop_duplicates()
