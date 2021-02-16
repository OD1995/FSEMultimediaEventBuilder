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
    sqlQuery_to_urlList,
    insert_EventBuilderProgress,
    update_EventBuilderProgress,
    ebs_stages
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

    ## Create a row in EventBuilderProgress
    _uuid_ = str(context.new_uuid())
    logging.info(f"_uuid_: {_uuid_}")
    stage_count = 1
    insert_EventBuilderProgress(
        uuid=_uuid_,
        utcNowStr=datetime.strftime(
            context.current_utc_datetime,
            "%Y-%m-%d %H:%M:%S.%f"
            ),
        sport=sport,
        event=event,
        ebs_stages=ebs_stages
    )

    ## Run query, get list of media URLs
    urlList = sqlQuery_to_urlList(inputs['query'])

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
    if len(imageList) > 0:
        ImagesIntoEventActivityResult = yield context.call_activity(
            'ImagesIntoEvent',
            {
                'imageList' : imageList,
                'sport' : sport,
                'event' : event
            }
        )
        logging.info(f"ImagesIntoEventActivityResult: {ImagesIntoEventActivityResult}")
    
    stage_count += 1
    update_EventBuilderProgress(
        uuid=_uuid_,
        utcNowStr=datetime.strftime(
        context.current_utc_datetime,
        "%Y-%m-%d %H:%M:%S.%f"
        ),
        stage="Images inserted into event",
        ebs_stages=ebs_stages,
        stage_count=stage_count
    )

    ## Add rows to AzureBlobVideos SQL table, add videos to us-office
    if len(videoList) > 0:
        VideosIntoEventActivityResult = yield context.call_activity(
            'VideosIntoEvent',
            {
                'videoList' : videoList,
                'sport' : sport,
                'event' : event
            }
        )
        logging.info(f"VideosIntoEventActivityResult: {VideosIntoEventActivityResult}")
    
    stage_count += 1
    update_EventBuilderProgress(
        uuid=_uuid_,
        utcNowStr=datetime.strftime(
        context.current_utc_datetime,
        "%Y-%m-%d %H:%M:%S.%f"
        ),
        stage="Videos inserted into event",
        ebs_stages=ebs_stages,
        stage_count=stage_count
    )

    update_EventBuilderProgress(
        uuid=_uuid_,
        utcNowStr=datetime.strftime(
        context.current_utc_datetime,
        "%Y-%m-%d %H:%M:%S.%f"
        ),
        done=True,
        stage_count=stage_count,
        ebs_stages=ebs_stages
    )

    


    return "done"

main = df.Orchestrator.create(orchestrator_function)