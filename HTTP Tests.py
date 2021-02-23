import requests
from datetime import datetime

badQuery1 = """
select top 100 * from MultimediaTest
where [filename] like '%o%'"""
badQuery2 = """
select top 100 container from MultimediaTest
where [filename] like '%o%'"""
goodQuery = """
select top 100 * from MultimediaTest
where [filename] like '%p%'"""

Q = """Select lower(concat('https://socialscraping.blob.core.windows.net/',
						downloadTo,
						'/',
						cast(MediaDownloadId as varchar(50)),
						'.',
						DownloadFileExtension)) as DownloadedMedia_AzureStorageURL
from [dbo].[MASTER_ClientArticles]   a Inner join [dbo].[Toolkit_MASTER_MediaDownloads] b On a.[Master_PostID] = b.[MasterRecordId]
Where B.downloaded is not null and B.DownloadedStatus is null 
and a.client = 'manUtd' and a.project = '2020-21 season' 
and A.[Article_TimeStamp] >= '2021-01-29 00:00:00.000'  
and A.[Article_TimeStamp] < '2021-01-30 00:00:00.000' 
and A.platform_ID = 2"""

badSport = "&!thisshouldfail!&"
goodSport = "testing"

goodEvent = "18Feb21 Test 3"

httpEndpoint1 = "https://fsemultimediaeventbuilder.azurewebsites.net/api/SQLQueryCheck"
httpEndpoint2 = "https://fsemultimediaeventbuilder.azurewebsites.net/api/EventBuilderTrigger"

## Make initial request
r = requests.post(
    httpEndpoint1,
    params={
        'query' : Q
    }
)
print(datetime.now())
print(r)
print(r.json())
assert r.json()['success']
print("first request done")
print(datetime.now())
### Make second request
r2 = requests.post(
    httpEndpoint2,
    params={
        'query' : Q,
        'sport' : goodSport,
        'event' : goodEvent,
        'sampling_proportion' : 0.001
    }      
)
print(r2)
#print(r2.json())
print(datetime.now())
print("second request done")
#r2js = r2.json()

#
### TERMINATE INSTANCES
#partitionKey = "99ddfa83c4d9498fb451a7ef408e574d"
#text = "going to change sampling proportion"
##
#terminateURL = f'https://fsemultimediaeventbuilder.azurewebsites.net/runtime/webhooks/durabletask/instances/{partitionKey}/terminate?reason={text}&taskHub=FSEMultimediaEventBuilder&connection=Storage&code=O3rTrA8H3NRuzKSGQSTdoTxMQvMb9seOv/vJmZkamAHaNvajPjuLqw=='
#print(datetime.now())
#od1 = requests.post(terminateURL)




