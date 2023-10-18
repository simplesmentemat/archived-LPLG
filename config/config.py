from decouple import config
API_HEADERS_AUTH = config('API_HEADERS_AUTH')
API_URL_ROOT = "https://open.tjstats.com/match-auth-app/open/"

API_HEADERS = {
    "Authorization": API_HEADERS_AUTH,
    "Content-Type": "application/json"
}

API_ENDPOINT_SCHEDULE = "v1/schedule/season?iOpen=-1"
API_ENDPOINT_STAGE = "v1/schedule/stage?"
API_ENDPOINT_MATCH = "v1/schedule/match"
API_ENDPOINT_DETAILS = "v1/compound/matchDetail?matchId="

