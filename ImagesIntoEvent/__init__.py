# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from azure.storage.blob import BlockBlobService, PublicAccess
import os
from MyFunctions import (
    get_url_container_and_file_name,
    get_SAS_URL
)


def main(inputDict: dict) -> str:
    logging.info("ImagesIntoEvent started")
    ## Get image list
    imageList = inputDict['imageList']
    sport = inputDict['sport']
    event = inputDict['event']
    logging.info(f"imageList len: {len(imageList)}")
    logging.info(f"sport: {sport}")
    logging.info(f"event: {event}")
    ## Create block blob services
    sourceBBS = BlockBlobService(
        connection_string=os.getenv("socialscrapingCS"))
    logging.info("source BBS created")
    destinationBBS = BlockBlobService(
        connection_string=os.getenv("fsecustomvisionimagesCS"))
    logging.info("dest BBS created")
    ## Ensure `sport` container exists
    destinationBBS.create_container(
            container_name=sport,
            public_access=PublicAccess.Blob,
            fail_on_exist=False)
    logging.info("new container created (if doesn't already exist)")
    ## Loop through list of images
    logging.info(f"start loading images into `{event}` folder of `{sport}` container")
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