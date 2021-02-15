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



## Overall Structure

## 1 - Run query, get list of media URLs
## 2 - Split into images and videos
## 3 - Create event and move images into it
## 4 - Add rows to AzureBlobVideos SQL table, add videos to us-office
## 5 - Run through Custom Vision model (if details provided) - NOT YET BUILT



def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Orchestrator started")

    ## Get the inputs
    inputs = json.loads(context._input)
    logging.info(f"inputs: {inputs}")
    sport = inputs['sport']
    event = inputs['event']

    ## Run query, get list of media URLs
    urlList = sqlQuery_to_urlList(inputs['sqlQuery'])

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

    ## Create event and move images into it
    ImagesIntoEventActivityResult = yield context.call_activity(
        'ImagesIntoEvent',
        {
            'imageList' : imageList,
            'sport' : sport,
            'event' : event
        }
    )
    logging.info(f"ImagesIntoEventActivityResult: {ImagesIntoEventActivityResult}")

    ## Add rows to AzureBlobVideos SQL table, add videos to us-office
    VideosIntoEventActivityResult = yield context.call_activity(
        'VideosIntoEvent',
        {
            'videoList' : videoList,
            'sport' : sport,
            'event' : event
        }
    )
    logging.info(f"VideosIntoEventActivityResult: {VideosIntoEventActivityResult}")

    


    return "done"

main = df.Orchestrator.create(orchestrator_function)