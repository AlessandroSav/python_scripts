import logging
import os
import sys
from datetime import datetime
from datetime import timezone

import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


class OpenDataAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}

    def __get_data(self, url, params=None):
        return requests.get(url, headers=self.headers, params=params).json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )


def download_file_from_temporary_download_url(download_url, filename,save_dir):
    """Download file from URL and save it to the specified directory."""
    os.makedirs(save_dir, exist_ok=True)  # Ensure directory exists
    filepath = os.path.join(save_dir, filename)
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filepath}")


def main():
    api_key = os.environ.get("KNMI_API_KEY") #KNMI_API_KEY is in .bashrc, do not hard-code tokens in the repo
    if not api_key:
        raise SystemExit(
            "Missing KNMI API token. Set it via environment variable KNMI_API_KEY (do not hard-code tokens in the repo)."
        )
    dataset_name = "cesar_tower_meteo_lc1_t10"
    dataset_version = "v1.1"
    logger.info(f"Fetching latest file of {dataset_name} version {dataset_version}")

    save_dir = "/perm/paaa/observations/Cesar"  # Change this to your desired save location


    api = OpenDataAPI(api_token=api_key)

    # sort the files in descending order and only retrieve the first file
    params = {"maxKeys": 1, "orderBy": "created", "sorting": "desc"}
    response = api.list_files(dataset_name, dataset_version, params)
    if "error" in response:
        logger.error(f"Unable to retrieve list of files: {response['error']}")
        sys.exit(1)

    latest_file = response["files"][0].get("filename")
    logger.info(f"Latest file is: {latest_file}")

    # fetch the download url and download the file
    response = api.get_file_url(dataset_name, dataset_version, latest_file)
    download_file_from_temporary_download_url(response["temporaryDownloadUrl"], latest_file,save_dir)


if __name__ == "__main__":
    main()