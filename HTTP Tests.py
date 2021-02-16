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

Q = "select * from MultimediaTest where [filename] like '%a%1dad%'"

badSport = "&!thisshouldfail!&"
goodSport = "test"

goodEvent = "16Feb21 Test 2"

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
        'event' : goodEvent
    }      
)
print(r2)
print(r2.json())
print(datetime.now())
print("second request done")
#r2js = r2.json()











