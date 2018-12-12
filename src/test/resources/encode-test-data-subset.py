#!/usr/bin/env python

# ASSUMPTIONS:
# encode data has been pulled down and is stored locally

maxFiles = 500;
# This script grabs only the subset of donors that have less than maxFiles files
# it outputs two files. The first with the subset of donors and the second with all the files for the subset of donors
dataDonors = open('encode-data-donors.json')
donorRows = dataDonors.readlines()
testDataDonors = open('encode-test-subset-data-donors.json', 'w+')

for donorRow in donorRows: # first we will collect the subset of donors
    donorId = donorRow[20: 31]
    dataFiles = open('encode-data-files.json')
    total = 0
    testDonor = donorRow
    for line in dataFiles:
        if donorId in line:
            total += 1
            if total > maxFiles:
                testDonor = ''
                break
    testDataDonors.write(testDonor)
    print donorId, total
    dataFiles.close()

dataDonors.close()
testDataDonors.close()

testDataDonors = open('encode-test-subset-data-donors.json') # then using the subset of donors, we can get the full set of files for each
testDonorRows = testDataDonors.readlines()
testDataFiles = open('encode-test-subset-data-files.json', 'w+')

for testDonorRow in testDonorRows:
    donorId = testDonorRow[20: 31]
    dataFiles = open('encode-data-files.json')
    for line in dataFiles:
        if donorId in line:
            testDataFiles.write(line)
    dataFiles.close()

testDataDonors.close()
testDataFiles.close()
