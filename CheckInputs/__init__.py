# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from MyFunctions import (
    check_container_name
)


def main(inputDict: dict) -> str:

    ## Ensure SQL query returns some rows
    someSQLrows = len(inputDict['urlList']) > 0
    ## Ensure sport can be used as a container
    _sport_ = check_container_name(inputDict['sport'])

    ## If any are those are not true, return list of errors
    allChecks = [
        someSQLrows,
        _sport_
    ]
    if False in allChecks:
        listOfErrors = []
        if not someSQLrows:
            textToAdd1 = "The SQL query provided returned 0 rows, please check it again and re-submit."
            listOfErrors.append(textToAdd1)
        if not _sport_:
            helpURL = "https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names"
            textToAdd2 = f"The `sport` input provided does not follow the naming convention rules for container, please read them ({helpURL}) and make the appropriate changes"
            listOfErrors.append(textToAdd2)
        
        return listOfErrors
    
    ## Otherwise, return the container name to use
    return {
        "sport" : _sport_
    }



