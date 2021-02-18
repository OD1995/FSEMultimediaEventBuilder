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
    insert_EventBuilderProgress
)
from datetime import datetime


def main(inputDict: dict) -> str:
    logging.info("AddRowToSQL started")
    ## Add row in SQL
    insert_EventBuilderProgress(
        uuid=inputDict['uuid'],
        utcNowStr=datetime.strftime(
            datetime.utcnow(),
            "%Y-%m-%dT%H:%M:%S"
            ),
        sport=inputDict['sport'],
        event=inputDict['event'],
        ebs_stages=inputDict['ebs_stages']
    )

    return "done"