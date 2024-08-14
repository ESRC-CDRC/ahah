from pathlib import Path

import pandas as pd


class Paths:
    DATA = Path("data")
    RAW = DATA / "raw"
    PROCESSED = DATA / "processed"
    OUT = DATA / "out"

    OPROAD = PROCESSED / "oproad"


class Config:
    POI_LIST = [
        "gpp",
        "dentists",
        "pharmacies",
        "hospitals",
        "greenspace",
        "bluespace",
    ]

    NHS_ENG_URL = "https://files.digital.nhs.uk/assets/ods/current/"
    NHS_ENG_FILES = {
        "gpp": "epraccur.zip",
        "dentists": "egdpprac.zip",
        "pharmacies": "edispensary.zip",
        "hospitals": "ets.zip",
    }
    NHS_SCOT_URL = "https://www.opendata.nhs.scot/api/3/action/datastore_search"
    NHS_SCOT_FILES = {
        "gpp": "b3b126d3-3b0c-4856-b348-0b37f8286367",
        "dentists": "3e848c81-758d-4d64-87ee-0e2f147a7a81",
        "pharmacies": "https://www.opendata.nhs.scot/dataset/a30fde16-1226-49b3-b13d-eb90e39c2058/resource/bfbc492d-7318-4b3d-9f01-087491aafb38/download/dispenser_contactdetails_may_24.csv",
        "hospitals": "c698f450-eeed-41a0-88f7-c1e40a568acc",
    }
    NHS_WALES_URL = (
        "https://nwssp.nhs.wales/ourservices/"
        "primary-care-services/primary-care-services-documents/"
    )
    NHS_WALES_FILES = {
        "pharmacies": (
            "pharmacy-practice-dispensing-data-docs"
            "/dispensing-data-report-november-2023"
        )
    }


def clean_air(path: Path, col: "str"):
    air: pd.DataFrame = pd.read_csv(path, skiprows=5, header=0)
    air = air[air[col] != "MISSING"]
    air[col] = air[col].astype(float)
    return air
