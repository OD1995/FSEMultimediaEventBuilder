import requests

badQuery = ""
goodQuery = ""

badSport = ""
goodSport = ""

eventName = "test"

httpEndpoint = ""

r = requests.post(
    httpEndpoint,
    data={
        'sqlQuery' : badQuery,
        'sport' : goodSport,
        'eventName' : eventName,
    }
)

print(r)
print(r.json())