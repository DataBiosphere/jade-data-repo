#!/bin/bash

# This script lists the commands to setup a BQ dataset to hold TestRunner results.

project_id="broad-jade-testrunnerresults"
dataset=test_runner_results

# list BQ datasets in the project
bq ls $project_id:

# create a new BQ dataset
bq --location=US mk -d --description "TestRunner results" $project_id:$dataset

# create the tables
bq mk --table $project_id:$dataset.testRun ./tableSchema_testRun.json
bq mk --table $project_id:$dataset.testScriptResults ./tableSchema_testScriptResults.json
bq mk --table $project_id:$dataset.measurementCollection ./tableSchema_measurementCollection.json

# list tables in the BQ dataset
bq ls $project_id:$dataset
