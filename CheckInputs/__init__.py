# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
from MyFunctions import (
    check_container_name,
    sqlQuery_to_urlList
)


def main(inputDict: dict) -> str:
    ## Ensure `sqlQuery` container "DownloadedMedia_AzureStorageURL"
    rightColumnName = "DownloadedMedia_AzureStorageURL" in inputDict['sqlQuery']
    if rightColumnName:
        ## Run query to get list of media URLs
        urlList = sqlQuery_to_urlList(inputDict['sqlQuery'])
        ## Ensure SQL query returns some rows
        someSQLrows = len(inputDict['urlList']) > 0
    else:
        someSQLrows = True
    ## Ensure sport can be used as a container
    _sport_ = check_container_name(inputDict['sport'])

    ## If any are those are not true, return list of errors
    allChecks = [
        rightColumnName,
        someSQLrows,
        _sport_
    ]
    if False in allChecks:
        listOfErrors = []
        if not rightColumnName:
            textToAdd0 = "The SQL query provided does not contain `DownloadedMedia_AzureStorageURL` in the SELECT statement, please add and re-submit"
            listOfErrors.append(textToAdd0)
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
        "sport" : _sport_,
        'urlList' : urlList
    }



