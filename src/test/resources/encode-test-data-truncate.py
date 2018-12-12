#!/usr/bin/env python

# ASSUMPTIONS:
# encode data has been pulled down and is stored locally

# This script grabs the full ~ 300 donors and gets up to 100 files for each
# it outputs one file with the truncated file list
dataDonors = open('encode-data-donors.json')
donorRows = dataDonors.readlines()
testDataFiles = open('encode-test-truncate-data-files.json', 'w+')

for donorRow in donorRows:
    donorId = donorRow[20: 31]
    dataFiles = open('encode-data-files.json')
    total = 0
    for line in dataFiles:
        if donorId in line:
            total += 1
            testDataFiles.write(line)
            if total > 99:
                break
    print donorId, total
    dataFiles.close()

dataDonors.close()
testDataFiles.close()
