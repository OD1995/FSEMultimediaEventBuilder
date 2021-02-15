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
    get_file_name_from_URL,
    run_sql_command,
    get_url_container_and_file_name,
    get_SAS_URL
)


def main(inputDict: dict) -> str:
    logging.info("ImagesIntoEvent started")
    ## Get image list
    videoList = inputDict['videoList']
    sport = inputDict['sport']
    event = inputDict['event']
    logging.info(f"videoList len: {len(videoList)}")
    logging.info(f"sport: {sport}")
    logging.info(f"event: {event}")
    ## # Create query to add rows to AzureBlobVideos SQL table
    ## Columns
    columnList = [
        # 'VideoID', - auto incrementing
        'VideoName',
        'Event',
        'Sport',
        'EndpointID',
        'MultipleVideoEvent'
    ]
    columnListString = ",".join([
        f"[{c}]"
        for c in columnList
    ])
    ## Values
    valuesList = [
        ",".join([
            f"'{get_file_name_from_URL(vidURL)}'",
            f"'{event}'",
            f"'{sport}'",
            "NULL",
            "TRUE"
        ])
        for vidURL in videoList
    ]
    valuesListString = "),(".join(valuesList)
    ## Build query
    insertQuery = f"""
    INSERT INTO AzureBlobVideos ({columnListString})
    VALUES ({valuesListString})
    """
    logging.info(f"AzureBlobVideos query: {insertQuery}")
    ## Run query
    run_sql_command(insertQuery)
    logging.info("query run")

    
    ## Upload videos to us-office


    ## Create block blob services
    sourceBBS = BlockBlobService(
        connection_string=os.getenv("socialscrapingCS"))
    logging.info("source BBS created")
    destinationBBS = BlockBlobService(
        connection_string=os.getenv("fsevideosCS"))
    logging.info("dest BBS created")

    ## Loop through list of videos
    for vidURL in videoList:
        urlContainer,urlFileName = get_url_container_and_file_name(vidURL)
        ## Create SAS URL
        sasURL = get_SAS_URL(
                            fileURL=vidURL,
                            block_blob_service=sourceBBS,
                            container=urlContainer
                        )
        ## Copy blob
        destinationBBS.copy_blob(
            container_name="us-office",
            blob_name=urlFileName,
            copy_source=sasURL
        )
    return "done"