#!/usr/bin/env python

import json, os, re, uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from datetime import datetime


auth, token = os.environ["AUTH_TOKEN"].split(": ", 1)
url_retrieve_snapshot = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/snapshots/"
thousand_genomes_search_snapshot_id = "77f1365e-b88d-48e2-a9ab-88ccd6cec68e"

def now():
    return datetime.now().isoformat()

#todo: refactor to use request library
def retrieve_snapshot():

    req = Request(url_retrieve_snapshot + thousand_genomes_search_snapshot_id)
    req.add_header("accept", "application/json")
    req.add_header(auth, token)

    try:
        res = urlopen(req)
    except HTTPError as err:
        if err.code == 401:
            raise err
        if err.code == 404:
            raise err

    snapshot = json.load(res)

    return snapshot

def generate_metadata():
    snapshot = retrieve_snapshot()
    obj = {
            "dct:identifier": snapshot["id"],
            "dct:title": "",
            "dct:description": "",
            "dct:creator":
                {
                    "foaf:name": "1000 Genomes"
                },
            "dct:issued": now(),
            "dct:modified": now(),
            "dcat:accessURL": "https://jade.datarepo-dev.broadinstitute.org/snapshots/details/{uuid}".format(uuid=snapshot["id"]),
            "TerraDCAT_ap:hasDataUsePermission": "TerraCore:NoRestriction",
            "TerraDCAT_ap:hasOriginalPublication":
                [
                    {
                        "dct:title": "Exploratory data analysis of genomic datasets using ADAM and Mango with Apache Spark on Amazon EMR",
                        "dcat:accessURL": "https://aws.amazon.com/blogs/big-data/exploratory-data-analysis-of-genomic-datasets-using-adam-and-mango-with-apache-spark-on-amazon-emr/"
                    }
                ],
            "TerraDCAT_ap:hasDataCollection": [
                {
                    "dct:identifier": "1000 Genomes",
                    "dct:title": "1000 Genomes",
                    "dct:description": "The 1000 Genomes Project is an international collaboration which has established the most detailed catalogue of human genetic variation, including SNPs, structural variants, and their haplotype context. The final phase of the project sequenced more than 2500 individuals from 26 different populations around the world and produced an integrated set of phased haplotypes with more than 80 million variants for these individuals.",
                    "dct:creator":
                        {
                            "foaf:name": "1000 Genomes"
                        },
                    "dct:publisher": "1000 Genomes",
                    "dct:issued": now(),
                    "dct:modified": now(),
                }
            ],
            "prov:wasGeneratedBy": [],
            "storage": snapshot['source'][0]['dataset']['storage'],
            "counts": {
                "donors": "",
                "samples": "",
                "files": "",
            },
            "files": "",
            "samples": "",
            "contributors": "",
        }
    collection = {"data": [obj]}
    print(json.dumps(collection, indent=4, sort_keys=True))
    return collection

upsert_url = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/search/{id}/metadata"
policy_url = "https://jade.datarepo-dev.broadinstitute.org/api/repository/v1/snapshots/{id}/policies/steward/members"


def api_request(id, url, obj, method):

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
    email = {"email": os.environ["USER_EMAIL"]}
    api_request(snapshot["dct:identifier"], policy_url, email, "POST")


def upsert(snapshot):
    api_request(snapshot["dct:identifier"], upsert_url, snapshot, "PUT")


collection = generate_metadata()
for snapshot in tqdm(collection["data"]):
    policy(snapshot)
    upsert(snapshot)


