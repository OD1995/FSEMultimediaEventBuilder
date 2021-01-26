# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
from collections import namedtuple
from datetime import datetime
from MyFunctions import (
    sqlQuery_to_urlList
)
import azure.functions as func
import azure.durable_functions as df



def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Orchestrator started")

    ## Get the inputs
    inputs = json.loads(context._input)
    sqlQuery = inputs['sqlQuery']

    ## Run query to get list of media URLs
    urlList = sqlQuery_to_urlList(sqlQuery)

    ## Split into images and videos
    imageExtensions = (
        '.jpg',
        '.jpeg',
        '.png'
    )
    imageList = [
        x
        for x in urlList
        if x.endswith(imageExtensions)
    ]
    videoExtensions = (
        '.mp4'
    )
    videoList = [
        x
        for x in urlList
        if x.endswith(videoExtensions)
    ]

    startUTCstr = datetime.strftime(context.current_utc_datetime,
                                        "%Y-%m-%d %H:%M:%S.%f")

    ## Use composite object if needed
    ##    - https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations?tabs=python#passing-multiple-parameters
    ## Call activity
    listOfFrameNumbers = yield context.call_activity(
                                    name='ReturnFrameNumbers',
                                    input_=videoDetails)



    ## Overall Structure

    ## 1 - Run query, get list of media URLs
    ## 2 - Split into images and videos
    ## 3 - Create event and move images into it
    ## 4 - Add rows to AzureBlobVideos SQL table, add videos to us-office
    ## 5 - Run through Custom Vision model (if details provided)
    


    return "this is a string"

main = df.Orchestrator.create(orchestrator_function)