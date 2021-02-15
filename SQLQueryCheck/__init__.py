import logging
import azure.functions as func
import azure.durable_functions as df
from MyFunctions import sqlQuery_to_urlList


async def main(
	req: func.HttpRequest,
    starter: str
):
    
    client = df.DurableOrchestrationClient(starter)
    ## Get SQL query from HTTP request
    sqlQuery = req.params.get("sqlQuery")

    ## Ensure `sqlQuery` container "DownloadedMedia_AzureStorageURL"
    rightColumnName = "DownloadedMedia_AzureStorageURL" in sqlQuery
    if rightColumnName:
        ## Run query to get list of media URLs
        urlList = sqlQuery_to_urlList(inputDict['sqlQuery'])
        ## Ensure SQL query returns some rows
        someSQLrows = len(inputDict['urlList']) > 0
    else:
        someSQLrows = True

    ## If any are those are not true, return list of errors
    allChecks = [
        rightColumnName,
        someSQLrows
    ]
    ## List to add errors to
    listOfErrors = []

    if False in allChecks:
        if not rightColumnName:
            textToAdd0 = "The SQL query provided does not contain `DownloadedMedia_AzureStorageURL` in the SELECT statement, please add and re-submit"
            listOfErrors.append(textToAdd0)
        if not someSQLrows:
            textToAdd1 = "The SQL query provided returned 0 rows, please check it again and re-submit."
            listOfErrors.append(textToAdd1)

    dictResponse = {
        'success' : True if len(listOfErrors) == 0 else False
        ,'errors' : listOfErrors
    }
    
    return func.HttpResponse(
        body=dictResponse
    )