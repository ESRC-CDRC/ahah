from pathlib import Path
from zipfile import ZipFile

import requests

from ahah.common.logger import logger
from ahah.common.utils import Config


def download_url(url: str, save_path: Path, chunk_size: int = 128):
    """
    Download a file from a url

    Parameters
    ----------
    url : str
        Full url for file download
    save_path : Path
        Path to save downloaded file
    chunk_size : int
        Size of download chunks to iteratively save
    """
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as fd:
        logger.debug(f"Starting download from {url}.")
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


if __name__ == "__main__":
    logger.info("Downloading NHS related data...")
    for name, url in Config.NHS_FILES.items():
        file = Config.RAW_DATA / "nhs" / f"{name}.zip"
        if not file.exists():
            download_url(Config.NHS_URL + url, save_path=file)
            logger.debug(f"{Config.NHS_URL + url} saved to {file}")
            with ZipFile(file, "r") as zip_ref:
                logger.debug(f"Unzipping {file}.")
                zip_ref.extractall(Config.RAW_DATA / "nhs")
                file.unlink()
        else:
            logger.warning(f"{file} exists: skipping {Config.NHS_URL + url}")

    for name, url in Config.NHS_SCOT_FILES.items():
        file = Config.RAW_DATA / "nhs" / "scotland" / f"{name}.csv"
        if not file.exists():
            download_url(Config.NHS_SCOT_URL + url, save_path=file)
            logger.debug(f"{Config.NHS_SCOT_URL + url} saved to {file}")
        else:
            logger.warning(f"{file} exists: skipping {Config.NHS_SCOT_URL + url}")

    for name, url in Config.NHS_WALES_FILES.items():
        file = Config.RAW_DATA / "nhs" / "wales" / f"{name}.xls"
        if not file.exists():
            download_url(Config.NHS_WALES_URL + url, save_path=file)
        else:
            logger.warning(f"{file} exists: skipping {Config.NHS_WALES_URL + url}")
