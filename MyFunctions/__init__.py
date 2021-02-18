
import logging
import pandas as pd
import pyodbc
from azure.storage.blob import ContainerPermissions
from datetime import datetime, timedelta
import re

## Database to query from, will be GlobalMultimedia eventually
##    but for testing it is AzureCognitive
DB = "GlobalMultimedia"
# DB = "AzureCognitive"

def sqlQuery_to_urlList(
    sqlQuery,
    database
):
    logging.info("sqlQuery_to_urlList started")
    df = get_df_from_sqlQuery(
        sqlQuery,
        database
    )
    ## Get column of interest as a list
    urlList = df['DownloadedMedia_AzureStorageURL'].to_list()

    return urlList

def get_df_from_sqlQuery(
    sqlQuery,
    database
):
    ## Create connection string
    connectionString = get_connection_string(database)
    logging.info(f'Connection string created: {connectionString}')
    ## Execute SQL query and get results into df 
    with pyodbc.connect(connectionString) as conn:
        ## Get SQL table in pandas DataFrame
        df = pd.read_sql(sql=sqlQuery,
                            con=conn)
    return df


def get_connection_string(database):
    username = 'matt.shepherd'
    password = "4rsenal!PG01"
    driver = '{ODBC Driver 17 for SQL Server}'
    # driver = 'SQL Server Native Client 11.0'
    server = "fse-inf-live-uk.database.windows.net"
    # database = 'AzureCognitive'
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    
    return connectionString

def get_container_from_URL(fileURL):
    return fileURL.split("/")[3]

def get_file_name_from_URL(fileURL):
    return "/".join(fileURL.split("/")[4:])


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

def check_container_name(
    sport
    ):
    ## # Make some adjustments to make the container name as ready as possible
    ## Convert all `sport` characters to lower case
    if sport is not None:
        isNotNone = True
        _sport_ = "".join([x.lower() if isinstance(x,str)
                            else "" if x == " " else x
                            for x in sport])
        ## Replace double hyphens
        _sport_ = _sport_.replace("--","-").replace("--","-")

        ## # Make some checks
        ## Check that the length is between 3 and 63 charachters
        length = (len(_sport_) >= 3) & (len(_sport_) <= 63)
        ## Check that all characters are either a-z, 0-9 or -
        rightCharTypes = True if re.match("^[a-z0-9-]*$", _sport_) else False
        ## Check that the first character is either a-z or 0-9
        firstCharRight = True if re.match("^[a-z0-9]*$", _sport_[0]) else False
        ## Check that the last character is either a-z or 0-9
        lastCharRight = True if re.match("^[a-z0-9]*$", _sport_[-1]) else False
    else:
        isNotNone = False
        length = False
        rightCharTypes = False
        firstCharRight = False
        lastCharRight = False
        _sport_ = ""


    if isNotNone & length & rightCharTypes & firstCharRight & lastCharRight:
        return  _sport_
    else:
        return False


def insert_EventBuilderProgress(
    uuid,
    utcNowStr,
    sport,
    event,
    ebs_stages
):
    Q = f"""
    INSERT INTO EventBuilderProgress
        (
            [EventBuildID]
            ,[StartUTC]
            ,[EndUTC]
            ,[LastUpdateUTC]
            ,[Sport]
            ,[Event]
            ,[Stage]
        )
    VALUES
        (
            '{uuid}'
            ,'{utcNowStr}'
            ,NULL
            ,'{utcNowStr}'
            ,'{sport}'
            ,'{event}'
            ,'1/{ebs_stages} - Progress row created'
        )
    """
    logging.info(Q)
    run_sql_command(
        sqlQuery=Q,
        database="AzureCognitive"
    )

## Progress stages for EventBuilderProgress
ebs_stages = 5
# 1 - Progress row created
# 2 - Images inserted into event
# 3 - Videos inserted into AzureBlobVideos
# 4 - Videos inserted into event
# 5 - Finished


def update_EventBuilderProgress(
    uuid,
    utcNowStr,
    ebs_stages,
    stage_count,
    stage=None,
    done=False
):
    ## If done == True, update the EndUTC column
    if done:        
        Q = f"""
        UPDATE      EventBuilderProgress
        SET         [EndUTC] = '{utcNowStr}'
                    ,[LastUpdateUTC] = '{utcNowStr}'
                    ,[Stage] = '5/5 - Finished'
        WHERE       [EventBuildID] = '{uuid}'
        """
    ## Otherwise, just update the 
    else:
        if stage_count:
            stageProg = f"{stage_count}/{ebs_stages} - {stage}"
        else:
            stageProg = stage
        Q = f"""
        UPDATE      EventBuilderProgress
        SET         [LastUpdateUTC] = '{utcNowStr}'
                    ,[Stage] = '{stageProg}'
        WHERE       [EventBuildID] = '{uuid}'
        """

    logging.info(Q)
    run_sql_command(
        sqlQuery=Q,
        database="AzureCognitive"
    )

def run_sql_command(
    sqlQuery,
    database
):
    ## Create connection string
    connectionString = get_connection_string(database)
    ## Run query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlQuery)