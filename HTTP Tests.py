import requests

badQuery1 = """
select top 100 * from MultimediaTest
where [filename] like '%o%'"""
badQuery2 = """
select top 100 container from MultimediaTest
where [filename] like '%o%'"""
goodQuery = """
select top 100 * from MultimediaTest
where [filename] like '%p%'"""

badSport = "&!thisshouldfail!&"
goodSport = "football"

eventName = "test"

httpEndpoint = "https://fsemultimediaeventbuilder.azurewebsites.net/api/HttpTrigger"

## Make initial request
r = requests.post(
    httpEndpoint,
    params={
        'sqlQuery' : badQuery1,
        'sport' : goodSport,
        'eventName' : eventName,
    }
)
print("first request done")
#print(r)
#print(r.json())
rURL = r.json()['statusQueryGetUri']
print(rURL)
## Make second request
r2 = requests.get(rURL)
print(r2)
print(r2.json())
r2js = r2.json()











