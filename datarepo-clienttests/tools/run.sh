# WARNING: Must be run from jade-data-repo
# Example: ./datarepo-clienttests/tools/run.sh shdev
#!/bin/bash

server=$1

if [ -z "$server" ]
then
  echo "buildClientRunSmokeTests: server cannot be empty"
  exit 1
fi

echo "Building Data Repo client library"
ENABLE_SUBPROJECT_TASKS=1 ./gradlew :datarepo-client:clean :datarepo-client:assemble
cd datarepo-clienttests

echo "Setting Test Runner environment variables"
export ORG_GRADLE_PROJECT_datarepoclientjar=$(find .. -type f -name "datarepo-client*.jar")
export TEST_RUNNER_SERVER_SPECIFICATION_FILE="${server}.json"
echo "ORG_GRADLE_PROJECT_datarepoclientjar = ${ORG_GRADLE_PROJECT_datarepoclientjar}"
echo "TEST_RUNNER_SERVER_SPECIFICATION_FILE = ${TEST_RUNNER_SERVER_SPECIFICATION_FILE}"

echo "Running spotless and spotbugs"
./gradlew spotlessCheck
./gradlew spotbugsMain

echo "Running test suite"
./gradlew runTest --args="suites/PRSmokeTests.json tmp/TestRunnerResults"

echo "Collecting measurements"
./gradlew collectMeasurements --args="PRSmokeTests.json tmp/TestRunnerResults"

echo "Uploading results"
./gradlew uploadResults --args="BroadJadeDev.json tmp/TestRunnerResults"
