#!/usr/bin/env python

import json, os, re, uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError

url_snapshots = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/snapshots?limit=1500"


def enumerate_snapshots():
    auth, token = os.environ["AUTH_TOKEN"].split(": ", 1)

    req = Request(url_snapshots)
    req.add_header("accept", "application/json")
    req.add_header(auth, token)

    try:
        res = urlopen(req)
    except HTTPError as err:
        if err.code == 401:
            raise err
        if err.code == 404:
            raise err

    snapshots = json.load(res)

    return snapshots


def filter_snapshots(snapshots):
    hca_snapshots = {}
    for snapshot in snapshots["items"]:
        regex = "hca_dev_([0-9a-f]{32})__[0-9]{8}_[0-9]{8}"

        m = re.search(regex, snapshot["name"])
        if m is None:
            continue

        project = str(uuid.UUID(m.group(1)))
        snapshot["project"] = project

        hca_snapshots[project] = snapshot

    return hca_snapshots


all_snapshots = enumerate_snapshots()
snapshots = filter_snapshots(all_snapshots)

with open("hca-snapshots.json", "w") as f:
    json.dump(snapshots, f, indent=2)
