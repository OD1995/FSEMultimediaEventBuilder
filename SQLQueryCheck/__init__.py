import logging
import azure.functions as func
import azure.durable_functions as df
from MyFunctions import get_df_from_sqlQuery
import json

async def main(
	req: func.HttpRequest,
    starter: str
):
    
    ## Get SQL query from HTTP request
    sqlQuery = req.params.get("query")
    # sqlQuery = req.form["query"]
    logging.info(f"sqlQuery: {sqlQuery}")
    ## Run query
    df = get_df_from_sqlQuery(sqlQuery)
    ## Ensure `sqlQuery` container "DownloadedMedia_AzureStorageURL"
    rightColumnName = "DownloadedMedia_AzureStorageURL" in df.columns
    ## Ensure SQL query returns some rows
    someSQLrows = len(df) > 0

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
        body=json.dumps(dictResponse)
    )