#!/usr/bin/env python

import json, os
from urllib.request import Request, urlopen
from urllib.error import HTTPError

api = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/search/{id}/metadata"


def endpoint(snapshot):
    auth, token = os.environ["AUTH_TOKEN"].split(": ", 1)

    url = api.format(id=snapshot["dct:identifier"])
    data = bytes(json.dumps(snapshot), "utf8")
    method = "PUT"

    req = Request(url, data=data, method=method)
    req.add_header("accept", "application/json")
    req.add_header(auth, token)

    try:
        res = urlopen(req)
    except HTTPError as err:
        if err.code == 401:
            raise err
        if err.code == 404:
            raise err


with open("hca-collection.json", "r") as f:
    collection = json.load(f)

for snapshot in collection["data"]:
    endpoint(snapshot)
