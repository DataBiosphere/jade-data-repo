#!/usr/bin/env python

import json
from urllib.request import urlopen
from urllib.error import HTTPError

# todo: refactor to use request library
url = "https://service.azul.data.humancellatlas.org/index/projects?catalog=dcp14&size=1000&sort=projectTitle&order=desc"

projects = []

while url:
    try:
        res = urlopen(url)
    except HTTPError as err:
        if err.code == 401:
            raise err
        if err.code == 404:
            print(err)
            continue
    obj = json.load(res)

    url = obj["pagination"]["next"]

    projects.append(obj)

with open("hca-projects.json", "w") as f:
    json.dump(projects, f, indent=2)
