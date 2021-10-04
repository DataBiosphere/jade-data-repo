#!/usr/bin/env python

import json, os
from urllib.request import Request, urlopen
from tqdm import tqdm

upsert_url = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/search/{id}/metadata"
policy_url = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/snapshots/{id}/policies/steward/members"


def auth_token():
    return os.environ["AUTH_TOKEN"].split(": ", 1)


def user_email():
    return os.environ["USER_EMAIL"]


def api_request(id, url, obj, method):
    auth, token = auth_token()

    url = url.format(id=id)
    data = json.dumps(obj).encode("utf-8")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        auth: token,
    }

    req = Request(url, data=data, headers=headers, method=method)
    res = urlopen(req)


def policy(snapshot):
    email = {"email": user_email()}
    api_request(snapshot["dct:identifier"], policy_url, email, "POST")


def upsert(snapshot):
    api_request(snapshot["dct:identifier"], upsert_url, snapshot, "PUT")


with open("hca-collection.json", "r") as f:
    collection = json.load(f)

for snapshot in tqdm(collection["data"]):
    policy(snapshot)
    upsert(snapshot)
