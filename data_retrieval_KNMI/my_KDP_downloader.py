# Try downloading surface fluxes
import logging
import requests
import os
import sys
from pathlib import Path

#Generic KNMI downloader for single to multiple files
def KNMI_KDP_downloader(date_time_list,filename,file_extension,outdir,api_url,api_key):

    '''
    In this function you can download data from the KNMI Data Platform by means of an API.
    Args:
    - date_time_list (str): list with date_times as string. Single entry is just one file.
    - filename (str): name of the specific file in string.
    - file_extension (str): extension of the file to be downloaded in string. E.g., '.nc'.
    - outdir (str): output directory.
    - api_url (str): KNMI API url in string. See link under "Access" tab when on the page of the dataset on dataplatform.knmi.nl
    - api_key (str): key to the KNMI dataplatform in string.

    Return:
    - .nc-file(s) of the requested dates.

    Written by Vincent de Feiter
    '''

    #check length
    length = len(date_time_list)

    if length < 2:
        print("Single file entered, now downloading")

        date_time=date_time_list[0]

        #Generate Logger
        logging.basicConfig()
        logger = logging.getLogger(__name__)
        logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

        #Complete filename
        if filename[-1] == '_':
            filename = filename[0:-2]
        filename_complete = filename+'_' +date_time  + file_extension
        print(filename_complete)

        #Download
        logger.debug(f"Dataset file to download: {filename_complete}")
        endpoint = f"{api_url}/{filename_complete}/url"

        get_file_response = requests.get(endpoint, headers={"Authorization": api_key})
        print(get_file_response)

        if get_file_response.status_code != 200:
            logger.error("Unable to retrieve download url for file")
            logger.error(get_file_response.text)
            sys.exit(1)

        logger.info(f"Successfully retrieved temporary download URL for dataset file {filename}")

        download_url = get_file_response.json().get("temporaryDownloadUrl")
        dataset_file_response = requests.get(download_url)

        # Write dataset file to disk
        p = Path(outdir,filename_complete)
        p.write_bytes(dataset_file_response.content)

        logger.info(f"Successfully downloaded dataset file to {p}")

        # Check logging for deprecation
        if "X-KNMI-Deprecation" in get_file_response.headers:
            deprecation_message = get_file_response.headers.get("X-KNMI-Deprecation")
            logger.warning(f"Deprecation message: {deprecation_message}")

    else:

        print("Multiple files entered, now downloading")

        for item in date_time_list:
            date_time = item

            #Generate Logger
            logging.basicConfig()
            logger = logging.getLogger(__name__)
            logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

            #Complete filename
            filename_complete = filename+"_"+date_time+file_extension
            print(filename_complete)
            #Download
            logger.debug(f"Dataset file to download: {filename_complete}")
            endpoint = f"{api_url}/{filename_complete}/url"

            get_file_response = requests.get(endpoint, headers={"Authorization": api_key})

            if get_file_response.status_code != 200:
                logger.error("Unable to retrieve download url for file")
                logger.error(get_file_response.text)
                sys.exit(1)

            logger.info(f"Successfully retrieved temporary download URL for dataset file {filename}")

            download_url = get_file_response.json().get("temporaryDownloadUrl")
            dataset_file_response = requests.get(download_url)

            # Write dataset file to disk
            p = Path(outdir,filename_complete)
            p.write_bytes(dataset_file_response.content)

            logger.info(f"Successfully downloaded dataset file to {p}")

            # Check logging for deprecation
            if "X-KNMI-Deprecation" in get_file_response.headers:
                deprecation_message = get_file_response.headers.get("X-KNMI-Deprecation")
                logger.warning(f"Deprecation message: {deprecation_message}")


if __name__ == "__main__":

   yyyymm_list = ['202205']
   dataset_name = "cesar_tower_meteo_lb1_t10"
   version = "v1.2"
   file_extension = ".nc"
   outdir = "/perm/paaa/observations/Cesar/"
   api_url = "https://api.dataplatform.knmi.nl/open-data/v1/datasets/cesar_tower_meteo_lb1_t10/versions/v1.2/files"
   api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImVlNDFjMWI0MjlkODQ2MThiNWI4ZDViZDAyMTM2YTM3IiwiaCI6Im11cm11cjEyOCJ9"

   KNMI_KDP_downloader(yyyymm_list, dataset_name + "_" + version, file_extension, outdir, api_url, api_key)