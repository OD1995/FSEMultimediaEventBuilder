# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
# from collections import namedtuple
# import cv2
# import math
# from datetime import datetime
# import json
# import tempfile
from azure.storage.blob import BlockBlobService
# import sys
import os
# sys.path.append(os.path.abspath('.'))
# import MyClasses
from MyFunctions import (
    get_url_container_and_file_name,
    get_SAS_URL
)
# import pytz

# vidDets = namedtuple('VideoDetails',
#                         ['blobDetails',
#                          'timeToCutUTC',
#                          'frameNumberList',
#                          'sport',
#                          'event'])


def main(inputDict: dict) -> str:
    logging.info("ImagesIntoEvent started")
    ## Get image list
    imageList = inputDict['imageList']
    sport = inputDict['sport']
    event = inputDict['event']
    ## Create block blob services
    sourceBBS = BlockBlobService(
        connection_string=os.getenv("socialscrapingCS"))
    destinationBBS = BlockBlobService(
        connection_string=os.getenv("fsecustomvisionimagesCS"))
    ## Ensure `sport` container exists

    ## Loop through list of images
    for imageURL in imageList:
        urlContainer,urlFileName = get_url_container_and_file_name(imageURL)
        ## Create SAS URL
        sasURL = get_SAS_URL(
                            fileURL=imageURL,
                            block_blob_service=sourceBBS,
                            container=urlContainer
                        )
        ## Copy blob
        destinationBBS.copy_blob(
            container_name=sport,
            blob_name=f"{event}/{urlFileName}",
            copy_source=sasURL
        )
    return "done"