#!/usr/bin/env python

import json
from datetime import datetime


def now():
    return datetime.now().isoformat()


def hca_creator():
    return {"foaf:name": "Human Cell Atlas"}


def access_url(snapshot):
    return (
        "https://jade.datarepo-dev.broadinstitute.org/snapshots/details/{uuid}".format(
            uuid=snapshot["id"]
        )
    )


def get_publications(project):
    publications = []
    for publication in project["publications"]:
        publications.append(
            {
                "dct:title": publication["publicationTitle"],
                "dcat:accessURL": publication["publicationUrl"],
            }
        )


def get_files(hit):
    files = []
    for file in hit["fileTypeSummaries"]:
        files.append(
            {
                "dcat:mediaType": file["fileType"],
                "dcat:byteSize": file["totalSize"],
                "count": file["count"],
            }
        )

    return files


def get_samples(hit):
    samples = []
    for sample in hit["samples"]:
        for id in sample["id"]:
            samples.append({"dct:identifier": id})

    return samples


def get_donors(hit):
    assert len(hit["donorOrganisms"]) <= 1

    genus = []
    disease = []

    for donor in hit["donorOrganisms"]:
        genus.extend(filter(None, donor["genusSpecies"]))
        disease.extend(filter(None, donor["disease"]))

    donors = {"genus": genus, "disease": disease}

    return donors


with open("hca-projects.json", "r") as f:
    projects = json.load(f)

with open("hca-snapshots.json", "r") as f:
    snapshots = json.load(f)

data = []
for hit in projects[0]["hits"]:
    assert len(hit["projects"]) == 1

    if hit["entryId"] not in snapshots:
        continue

    project = hit["projects"][0]
    snapshot = snapshots[project["projectId"]]

    get_samples(hit)

    obj = {
        "dct:identifier": snapshot["id"],
        "dct:title": project["projectTitle"],
        "dct:description": project["projectDescription"],
        "dct:creator": hca_creator(),
        "dct:issued": project["submissionDate"],
        "dct:modified": project["updateDate"],
        "dcat:accessURL": access_url(snapshot),
        "TerraDCAT_ap:hasDataUsePermission": "TerraCore:NoRestriction",
        "TerraDCAT_ap:hasOriginalPublication": get_publications(project),
        "TerraDCAT_ap:hasDataCollection": [
            {
                "dct:identifier": "HCA",
                "dct:title": "Human Cell Atlas",
                "dct:description": "The Human Cell Atlas (HCA) data collection contains comprehensive reference maps of all human cells - the fundamental units of life - as a basis for understanding fundamental human biological processes and diagnosing, monitoring, and treating disease.",
                "dct:creator": hca_creator(),
                "dct:publisher": "Data Explorer Team",
                "dct:issued": now(),
                "dct:modified": now(),
            }
        ],
        "storage": snapshot["storage"],
        "counts": {
            "donors": sum(d["donorCount"] for d in hit["donorOrganisms"]),
            "samples": sum(len(s["id"]) for s in hit["samples"]),
            "files": sum(f["count"] for f in hit["fileTypeSummaries"]),
        },
        "files": get_files(hit),
        "donors": get_donors(hit),
        "contributors": project["contributors"],
    }
    data.append(obj)

collection = {"data": data}

with open("hca-collection.json", "w") as f:
    json.dump(collection, f, indent=2)
