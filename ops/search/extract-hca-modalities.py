#!/usr/bin/env python

import csv
import json

with open("hca-projects.json", "r") as f:
    projects = json.load(f)

assert len(projects) == 1

product = []
for hit in projects[0]["hits"]:
    assays = []
    source = []
    for protocol in hit["protocols"]:
        hasLib = "libraryConstructionApproach" in protocol
        hasSrc = "nucleicAcidSource" in protocol
        assert hasLib == hasSrc
        if hasLib:
            for assay in protocol["libraryConstructionApproach"]:
                assays.append(assay)
        if hasSrc:
            for nucleic in protocol["nucleicAcidSource"]:
                source.append(nucleic)
    product.extend([(lib, src) for lib in assays for src in source])

product = list(set(product))  # remove duplicates

with open("hca-modalities.csv", "w") as f:
    writer = csv.writer(f)
    header = ("library_construction_approach", "nucleic_acid_source")
    writer.writerow(header)
    for row in product:
        writer.writerow(row)
