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
    ebs_stages,
    DB
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
    samplingProportion = inputs['sampling_proportion']

    ## Create a row in EventBuilderProgress
    _uuid_ = str(context.new_uuid())
    logging.info(f"_uuid_: {_uuid_}")
    stage_count = 1
    AddRowToSQLResult = yield context.call_activity(
            'AddRowToSQL',
            {
                'sport' : sport,
                'event' : event,
                'ebs_stages' : ebs_stages,
                'uuid' : _uuid_
            }
        )
    logging.info(f"AddRowToSQLResult: {AddRowToSQLResult}")

    ## Run query, get list of media URLs
    urlList = sqlQuery_to_urlList(
        sqlQuery=inputs['query'],
        database=DB
    )

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
    logging.info(f"{len(imageList)} images in the query")
    videoExtensions = (
        '.mp4'
    )
    videoList = [
        x
        for x in urlList
        if x.endswith(videoExtensions)
    ]
    logging.info(f"{len(videoList)} videos in the query")

    ## Create event and move images into it
    stage_count += 1
    if len(imageList) > 0:
        ImagesIntoEventActivityResult = yield context.call_activity(
            'ImagesIntoEvent',
            {
                'imageList' : imageList,
                'sport' : sport,
                'event' : event,
                'samplingProportion' : samplingProportion,
                'ebs_stages' : ebs_stages,
                'stage_count' : stage_count,
                'uuid' : _uuid_
            }
        )
        logging.info(f"ImagesIntoEventActivityResult: {ImagesIntoEventActivityResult}")
    


    ## Add rows to AzureBlobVideos SQL table, add videos to us-office
    stage_count += 1
    if len(videoList) > 0:
        VideosIntoEventActivityResult = yield context.call_activity(
            'VideosIntoEvent',
            {
                'videoList' : videoList,
                'sport' : sport,
                'event' : event,
                'ebs_stages' : ebs_stages,
                'stage_count' : stage_count,
                'uuid' : _uuid_
            }
        )
        logging.info(f"VideosIntoEventActivityResult: {VideosIntoEventActivityResult}")

    ## Monitor the AzureBlobVideoCompletes table to see when all submitted videos are JPEGed
    stage_count += 1
    DetectVideoFinishResult = yield context.call_activity(
        'DetectVideoFinish',
        {
            'videoList' : videoList,
            'sport' : sport,
            'event' : event,
            'ebs_stages' : ebs_stages,
            'stage_count' : stage_count,
            'uuid' : _uuid_
        }
    )
    logging.info(f"DetectVideoFinishResult: {DetectVideoFinishResult}")
    

    update_EventBuilderProgress(
        uuid=_uuid_,
        utcNowStr=datetime.strftime(
        context.current_utc_datetime,
        "%Y-%m-%dT%H:%M:%S"
        ),
        done=True,
        stage_count=stage_count,
        ebs_stages=ebs_stages
    )
    


    return "done"

main = df.Orchestrator.create(orchestrator_function)