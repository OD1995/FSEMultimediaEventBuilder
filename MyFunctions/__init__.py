
import logging
import pandas as pd
import pyodbc
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta

def sqlQuery_to_urlList(
    sqlQuery
):
    logging.info("sqlQuery_to_urlList started")
    ## Create connection string
    connectionString = get_connection_string()
    logging.info(f'Connection string created: {connectionString}')
    ## Execute SQL query and get results into df 
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    ## Get column of interest as a list
    urlList = df['FileUrl'].to_list()

    return urlList


def get_connection_string():
    username = 'matt.shepherd'
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    server = "fse-inf-live-uk.database.windows.net"
    database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    
    return connectionString

def get_container_from_URL(fileURL):
    return fileURL.split("/")[3]

def get_file_name_from_URL(fileURL):
    return "/".join(fileURL.split("/")[4:])

# def get_SAS_URL_using_container(
#     fileURL,
#     block_blob_service
# ):
#     ## Get container
#     container = get_container_from_URL(fileURL)
#     ## Get SAS URL
#     sasURL = get_SAS_URL(
#                 fileURL=fileURL,
#                 block_blob_service=block_blob_service,
#                 container=container)
#     return sasURL

def get_url_container_and_file_name(fileURL):
    return get_container_from_URL(fileURL),get_file_name_from_URL(fileURL)

def get_SAS_URL(fileURL,
                block_blob_service,
                container):

    sasTokenRead = block_blob_service.generate_container_shared_access_signature(
    container_name=container,
    permission=ContainerPermissions.READ,
    expiry=datetime.utcnow() + timedelta(days=1)
    )
    return f"{fileURL}?{sasTokenRead}"