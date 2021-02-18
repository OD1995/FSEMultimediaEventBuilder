# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from MyFunctions import (
    update_EventBuilderProgress,
    get_df_from_sqlQuery,
    get_file_name_from_URL
)
from datetime import datetime
from time import sleep


def main(inputDict: dict) -> str:
    logging.info("DetectVideoFinish started")
    
    event = inputDict['event']
    sport = inputDict['sport']
    videoList = inputDict['videoList']

    ## Every X seconds, run a query to check if the expected number
    ##    of videos have been added to AzureBlobVideoCompletes. Once
    ##    they're added, we can assume the 
    every_X_seconds = 30
    timeout_hours = 5
    timeout_seconds = 60 * 60 * timeout_hours

    videoNames = [
        get_file_name_from_URL(x)
        for x in videoList
    ]

    whereClauseString = "\n OR \n".join([
        f"(([VideoName] = '{vn}') AND ([Event] = '{event}') AND ([OutputContainer] = '{sport}'))"
        for vn in videoNames
    ])

    ## Build SQL query
    sqlQuery = f"""
    SELECT      *
    FROM        AzureBlobVideoCompletes
    WHERE       {whereClauseString}               
    """

    logging.info(f"sqlQuery: {sqlQuery}")

    start_time = datetime.now()

    if len(videoList) > 0:

        while True:
            ## Make sure we haven't timed out
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                update_EventBuilderProgress(
                    uuid=inputDict['uuid'],
                    utcNowStr=datetime.strftime(
                        datetime.utcnow(),
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    stage="ERROR - Video JPEGing timed out",
                    ebs_stages=None,
                    stage_count=None
                )
                S = f"{timeout_seconds} seconds"
                H = f"{timeout_hours} hours"
                raise TimeoutError(f"Timed out, while loop happening for more than {S}/{H}")
            ## Run the query
            df = get_df_from_sqlQuery(
                sqlQuery=sqlQuery,
                database="AzureCognitive"
            )
            logging.info(f"len(df): {len(df)}")
            logging.info(f"required: {len(videoList)}")
            ## If we have enough rows, break out
            if len(df) == len(videoList):
                logging.info("all rows in, break out of while loop")
                break
            ## Otherwise, sleep for a bit
            sleep(every_X_seconds)

    ## Update row in SQL
    logging.info("All the videos have been JPEGed")
    update_EventBuilderProgress(
        uuid=inputDict['uuid'],
        utcNowStr=datetime.strftime(
            datetime.utcnow(),
            "%Y-%m-%dT%H:%M:%S"
        ),
        stage="Videos inserted into event",
        ebs_stages=inputDict['ebs_stages'],
        stage_count=inputDict['stage_count']
    )

    return "done"