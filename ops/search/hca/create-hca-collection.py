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
    return publications


def get_files(hit):
    files = []
    for file in hit["fileTypeSummaries"]:
        files.append(
            {
                "dcat:mediaType": file["format"],
                "dcat:byteSize": file["totalSize"],
                "count": file["count"],
            }
        )

    return files


def get_sample_ids(hit):
    samples = []
    for sample in hit["samples"]:
        for id in sample["id"]:
            samples.append({"dct:identifier": id})

    return samples


def get_modalities(hit):
    modalityMap = {
        "10x sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10X 3' v1 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x 3' v2": ["TerraCoreValueSets:Transcriptomic"],
        "10x 3' v2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10X 3' v2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10X 3' V2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x 3' V2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x 3' v3": ["TerraCoreValueSets:Transcriptomic"],
        "10x 3' v3 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10X 3' v3 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x 5' v1": ["TerraCoreValueSets:Transcriptomic"],
        "10X 5' v2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x v2 3'": ["TerraCoreValueSets:Transcriptomic"],
        "10x v2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10X v2 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "10x v3 sequencing": ["TerraCoreValueSets:Transcriptomic"],
        "CITE 10x 3' v2": [
            "TerraCoreValueSets:Transcriptomic",
            "TerraCoreValueSets:Proteomic",
        ],
        "CITE-seq": [
            "TerraCoreValueSets:Transcriptomic",
            "TerraCoreValueSets:Proteomic",
        ],
        "Smart-seq": ["TerraCoreValueSets:Transcriptomic"],
        "Smart-Seq": ["TerraCoreValueSets:Transcriptomic"],
        "Smart-seq2": ["TerraCoreValueSets:Transcriptomic"],
        "Smart-like": ["TerraCoreValueSets:Transcriptomic"],
        "Drop-seq": ["TerraCoreValueSets:Transcriptomic"],
        "Drop-Seq": ["TerraCoreValueSets:Transcriptomic"],
        "10X Feature Barcoding technology for cell surface proteins": [
            "TerraCoreValueSets:Transcriptomic",
            "TerraCoreValueSets:Proteomic",
        ],
        "10X Gene Expression Library": [
            "TerraCoreValueSets:Transcriptomic",
            "TerraCoreValueSets:Proteomic",
        ],
        "10x Ig enrichment": ["TerraCoreValueSets:Transcriptomic"],
        "10X Ig enrichment": ["TerraCoreValueSets:Transcriptomic"],
        "10x TCR enrichment": ["TerraCoreValueSets:Transcriptomic"],
        "10X TCR enrichment": ["TerraCoreValueSets:Transcriptomic"],
        "Fluidigm C1-based library preparation": ["TerraCoreValueSets:Transcriptomic"],
        "barcoded plate-based single cell RNA-seq": [
            "TerraCoreValueSets:Transcriptomic"
        ],
        "cDNA library construction": ["TerraCoreValueSets:Transcriptomic"],
        "ATAC 10x v1": ["TerraCoreValueSets:Epigenomic"],
        "inDrop": ["TerraCoreValueSets:Transcriptomic"],
        "DNA library construction": ["TerraCoreValueSets:Genomic"],
        "sci-CAR": ["TerraCoreValueSets:Transcriptomic"],
        "sci-RNA-seq": ["TerraCoreValueSets:Transcriptomic"],
        "DroNc-Seq": ["TerraCoreValueSets:Transcriptomic"],
        "MARS-seq": ["TerraCoreValueSets:Transcriptomic"],
    }

    categoryMap = {
        "single cell": "scRNA-seq",
        "single nucleus": "snRNA-seq",
        "bulk cell": "RNA-seq",
        "bulk nuclei": "nuc-seq",
    }

    assays = []
    categories = []
    modalities = []
    for protocol in hit["protocols"]:
        if "libraryConstructionApproach" in protocol:
            for assay in protocol["libraryConstructionApproach"]:
                assert assay in modalityMap
                assays.append(assay)
                modalities.extend(modalityMap[assay])
        if "nucleicAcidSource" in protocol:
            for source in protocol["nucleicAcidSource"]:
                categories.append(categoryMap[source])

    return list(set(assays)), list(set(modalities)), list(set(categories))


def get_genus_disease(hit):
    assert len(hit["donorOrganisms"]) in [0, 1]

    genus = []
    disease = []

    for donor in hit["donorOrganisms"]:
        genus.extend(filter(None, donor["genusSpecies"]))
        disease.extend(filter(None, donor["disease"]))

    return {"genus": genus, "disease": disease}


with open("hca-projects.json", "r") as f:
    projects = json.load(f)

with open("hca-snapshots.json", "r") as f:
    snapshots = json.load(f)

assert len(projects) == 1

data = []
for hit in projects[0]["hits"]:
    assert len(hit["projects"]) == 1

    if hit["entryId"] not in snapshots:
        continue

    project = hit["projects"][0]
    dates = hit["dates"][0]
    snapshot = snapshots[project["projectId"]]

    get_sample_ids(hit)

    assays, modalities, categories = get_modalities(hit)

    obj = {
        "dct:identifier": snapshot["id"],
        "dct:title": project["projectTitle"],
        "dct:description": project["projectDescription"],
        "dct:creator": hca_creator(),
        "dct:issued": dates["submissionDate"],
        "dct:modified": dates["updateDate"],
        "dcat:accessURL": access_url(snapshot),
        "TerraDCAT_ap:hasDataUsePermission": ["TerraCore:NoRestriction"],
        "TerraDCAT_ap:hasOriginalPublication": get_publications(project),
        "TerraDCAT_ap:hasDataCollection": [
            {
                "dct:identifier": "HCA",
                "dct:title": "Human Cell Atlas",
                "dct:description": "The Human Cell Atlas (HCA) data collection contains comprehensive reference maps of all human cells - the fundamental units of life - as a basis for understanding fundamental human biological processes and diagnosing, monitoring, and treating disease.",
                "dct:creator": hca_creator(),
                "dct:publisher": "Human Cell Atlas",
                "dct:issued": now(),
                "dct:modified": now(),
            }
        ],
        "prov:wasGeneratedBy": [
            {
                "TerraCore:hasAssayType": assays,
            },
            {
                "TerraCore:hasDataModality": modalities,
            },
            {
                "TerraCore:hasAssayCategory": categories,
            },
        ],
        "storage": snapshot["storage"],
        "counts": {
            "donors": sum(d["donorCount"] for d in hit["donorOrganisms"]),
            "samples": sum(len(s["id"]) for s in hit["samples"]),
            "files": sum(f["count"] for f in hit["fileTypeSummaries"]),
        },
        "files": get_files(hit),
        "samples": get_genus_disease(hit),
        "contributors": project["contributors"],
    }
    data.append(obj)

collection = {"data": data}

with open("hca-collection.json", "w") as f:
    json.dump(collection, f, indent=2)
