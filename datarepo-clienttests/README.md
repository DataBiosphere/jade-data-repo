# datarepo-clienttests
This sub-project of the jade-data-repo repository contains infrastructure for running tests with the Data Repository client library.

Relevant documents:
* [Performance Testing Infrastructure Proposal](https://docs.google.com/document/d/11PZIXZwOyd394BFOlBsDjOGjZdC-jwTr_n92nTJvFxw)
* [Concurrency & Profiling Epic](https://docs.google.com/document/d/12xoTnpgiUnwdIW2qSc6W5nZOWCaOE-ZNpK_0KkyxQcg/)

Jump to sections below:
* [Terminology](#Terminology)
* [Execute a test run](#Execute-a-test-run)
* [Write a new test](#Write-a-new-test)
* [Troubleshooting](#Troubleshooting)
* [Build out the testing infrastructure](#Build-out-the-testing-infrastructure)

## Terminology
This testing infrastructure aims to separate the test from the system configuration that it runs in. This is so that
it's easier to profile variations of API calls and environment without rewriting a lot of test code.

#### Test Script
A test script contains several API calls to perform some user action(s) or user journey. They can be broken down into
three parts:
  1. Setup (e.g. Create a dataset)
  2. User Journey (e.g. Make one bulk file load API call)
  3. Cleanup (e.g. Delete dataset)

The User Journey part contains the API call(s) that we want to profile and it may be scaled to run multiple journeys in
parallel. By contrast, the Setup and Cleanup parts contain the API call(s) that we do not want to profile and will not
be scaled to run multiple in parallel. For the example above, we could run multiple bulk file loads in parallel to test
the performance, but the dataset creation and deletion would be done only once.

#### Test Configuration
A test configuration describes how to set up the test environment, which test script(s) to run, how to scale the test
script(s), and how to add stress into the system.

The environment specification includes settings for:
  * Deployment (e.g. developer’s dev namespace, performance testing Kubernetes cluster, how to deploy there)
  * Kubernetes (e.g. initial size of replica set)
  * Application (e.g. Stairway thread pool size, maximum number of bulk file loads)

The scripts to run specification includes the:
  * Set of different User Journeys to run (e.g. bulk file load & DRS lookup, just DRS lookups) and any parameters they
  require (e.g. source/destination bucket region, number of files per bulk load).
  * Number of each User Journey to run (e.g. 500 bulk file loads & 300 DRS lookups, 1000 DRS lookups)
  * Timeout of client threads (e.g. each bulk file load should take < 15 minutes, each DRS lookup should take < 30 seconds)

The scaling specification includes:
  * Number of different users making the User Journey calls.

**This section will be updated as more pieces of the test configuration are implemented.** See the
[Performance Testing Infrastructure Proposal](https://docs.google.com/document/d/11PZIXZwOyd394BFOlBsDjOGjZdC-jwTr_n92nTJvFxw)
for more details on the desired end goal.

#### Test Run
A test run is a single execution of a test configuration.

#### Test Runner
The test runner executes test configurations. The steps involved in each test run are:
  * Re-deploy the API (i.e. Helm delete then upgrade) with the application properties specified by the configuration.
  * Modify the Kubernetes environment, as specified by the configuration.
  * Run the Setup for each test script.
  * Create a client thread pool for each test script specification.
  * Kick off some number of threads, each running one User Journey, as specified by the configuration.
  * Wait until all threads either finish or time out.
  * Run the Cleanup for each test script.
  * Teardown the API deployment, if applicable.

The implementation of the test runner is where the bulk of the testing infrastructure code lives.

#### Test Suite
A test suite is a collection of test configurations that have some similar purpose. For example, a smoke test suite to
detect major performance problems quickly or a very long running suite to detect possible memory leaks. The test
configurations are run serially.

## Execute a test configuration or suite
Find a test configuration or suite to execute. Each configuration is a JSON file in the resources/configs directory.

Call the Gradle runTest task and pass it the name of the test configuration or suite to execute, 
along with the output directory to write the results to.

`./gradlew runTest --args="configs/basicexamples/BasicUnauthenticated.json /tmp/TestRunnerResults"`

`./gradlew runTest --args="suites/BasicSmoke.json /tmp/TestRunnerResults"`

The environment variable TEST_RUNNER_SERVER_SPECIFICATION_FILE optionally overrides the server configuration specified
by either the test suite or configuration. The server specification is determined by the following, in order:
  1. environment variable
  2. test suite server property (if running a test suite)
  3. test configuration server property
  
`TEST_RUNNER_SERVER_SPECIFICATION_FILE="mmdev.json" ./gradlew run --args="configs/basicexamples/LookupDataset.json"`

## Collect measurements generated by the server during a test run
Find the list of measurements to collect. Each list is a JSON file in the resources/measurementlists directory.
There are 2 options for how to call the collectMeasurements task.

Call the Gradle collectMeasurements task and pass it the list of measurements to collect, the output directory to write 
the results to, the server to query, and the start and end timestamps for the time interval.

`./gradlew collectMeasurements --args="AllMeasurements.json /tmp/TestRunnerResults mmdev.json '2020-08-20 13:18:34' '2020-08-20 13:18:35.615628881'"`

Call the Gradle collectMeasurements task and pass it the list of measurements to collect, and the output directory to
write the results to. If the output directory is the same one where a test run results are written, then it will pull
the start and end timestamps from the summary file for the test run.

`./gradlew collectMeasurements --args="BasicKubernetes.json /tmp/TestRunnerResults"`


#### Run against a local server
There is a localhost.json server specification file in the resources/server directory. This file contains a filepath to
the top-level directory of the jade-data-repo Git repository. Executing a test against this configuration, with the path
modified for your own machine, will start a local Data Repo server by executing the Gradle bootRun task from that
directory.

This is useful for debugging or testing local server code changes.

#### Use a local Data Repo client JAR file
The version of the Data Repo client JAR file is specified in the build.gradle file in this sub-project. This JAR file is
fetched from the Broad Institute Maven repository. You can override this to use a local version of the Data Repo client
JAR file by specifying a Gradle project property, either with a command line argument

`./gradlew -Pdatarepoclientjar=/Users/marikomedlock/Workspaces/jade-data-repo/datarepo-client/build/libs/datarepo-client-1.0.39-SNAPSHOT.jar run --args="configs/BasicUnauthenticated.json`

or an environment variable.

`export ORG_GRADLE_PROJECT_datarepoclientjar=../datarepo-client/build/libs/datarepo-client-1.0.39-SNAPSHOT.jar`
`./gradlew run --args="configs/BasicUnauthenticated.json`

This is useful for debugging or testing local server code changes that affect the generated client library (e.g. new API
endpoint). You can generate the Data Repo client library with the Gradle assemble task of the datarepo-client sub-project.

`cd /Users/marikomedlock/Workspaces/jade-data-repo/datarepo-client`

`../gradlew clean assemble`

`ls -la ./build/libs/*jar`

## Write a new test
#### Add a new test configuration
A test configuration is an instance of the TestConfiguration POJO class, serialized into JSON and saved in the
resources/configs directory. Below are the available fields:
  * name: Name of the configuration
  * description: (optional) Description of the configuration
  * serverSpecificationFile: Name of a file in the resources/servers directory that specifies the server to test against
  * billingAccount: Google billing account to use
  * kubernetes: Kubernetes-related settings that will be set after deploying the application and before executing any
  tests
    * numberOfInitialPods: (optional) Initial number of pods, defaults to 1
  * application: Application-related settings that will be set before deploying the application and executing any tests
    * maxStairwayThreads: (optional) defaults to 20
    * maxBulkFileLoad: (optional) defaults to 1000000
    * loadConcurrentFiles: (optional) defaults to 80
    * loadConcurrentIngests: (optional) defaults to 2
    * inKubernetes: (optional) defaults to false
    * loadHistoryCopyChunkSize: (optional) defaults to 1000
    * loadHistoryWaitSeconds: (optional) defaults to 2
    * loadDriverWaitSeconds: (optional) defaults to 1
  * testScripts: List of test script specifications (i.e. instance of the TestScriptSpecification POJO class, serialized
  into JSON). Each specification should include the below required fields:
    * name: Name of the test script class to run
    * totalNumberToRun: Integer number of user journeys to run
    * numberToRunInParallel: Integer number of user journeys to run in parallel (i.e. size of the thread pool)
    * expectedTimeForEach: Integer number of time units indicating the maximum amount of time a user journey thread will
    be allowed to execute.
    * expectedTimeForEachUnit: String representation of the Java TimeUnit class (e.g. MILLISECONDS, SECONDS, MINUTES)
  * testUserFiles: List of names of files in the resources/testusers directory that specify the users whose crendentials
  will be used to run the test scripts

#### Add a new test script
A test script is a sub-class of the TestScript base class. It specifies the setup and cleanup methods that are run once
at the beginning and end of a test run, respectively. It also specifies the userJourney method, which will be launched
in multiple threads in parallel, as specified by the test configuration.

#### Add a new server specification
A server specification is an instance of the ServerSpecification POJO class, serialized into JSON and saved in the
resources/servers directory. Below are the available fields:
  * name: Name of the server
  * description: (optional) Description of the server
  * uri: URI of the Data Repo instance
  * clusterName: Name of the Kubernetes cluster where the Data Repo instance is deployed
  * clusterShortName: Name of the cluster, stripped of the region and project qualifiers
  * region: Region where the cluster is running
  * project: Google project under which the cluster is running
  * namespace: (optional) Name of the Kubernetes namespace where the Data Repo instance is deployed
  * deploymentScript: Name of the deployment script class to run. Only required if skipDeployment is false
  * skipDeployment: (optional) true to skip the deployment script, default is false
  * skipKubernetes: (optional) true to skip the post-deployment Kubernetes modifications, default is false

#### Add a new deployment script
A deployment script is a sub-class of the DeploymentScript base class. It specifies the deploy, waitForDeployToFinish,
and optional teardown methods that are run once at the beginning and end of a test run, respectively.

#### Add a new test user specification
A test user specification is an instance of the TestUserSpecification POJO class, serialized into JSON and saved in the
resources/testusers directory. Below are the required fields:
  * name: Name of the test user
  * userEmail: Email of the test user
  * delegatorServiceAccountFile: Name of a file in the resources/serviceaccounts directory that specifies the service
  account with permission to fetch domain-wide delegated credentials for the test user

All test users must be registered in Terra and there must be a service account that can fetch domain-wide delegated
credentials for the user. Jade has already setup test users (see
src/main/resources/application-integrationtest.properties) and the jade-k8-sa service account to delegate for them. It's
probably easiest to reuse one of these test users when adding new tests.

#### Add a new service account specification
A service account specification is an instance of the ServiceAccountSpecification POJO class, serialized into JSON and
saved in the resources/serviceaccounts directory. Below are the required fields:
  * name: Name of the service account
  * serviceAccountEmail: Email of the service account
  * jsonKeyFilePath: JSON-formatted file that includes the client ID and private key
  * pemFilePath: PEM file for the service account

The JSON key file and PEM files for the jade-k8-sa service account match the paths used by the render-configs script in
the main datarepo project. Jade stores these files in Vault and uses the script to fetch them locally for each test run.

#### Add a new test suite
A test suite is an instance of the TestSuite POJO class, serialized into JSON and saved in the resources/suites
directory. Below are the available fields:
  * name: Name of the test suite
  * description: (optional) Description of the test suite
  * serverSpecificationFile: Name of a file in the resources/servers directory that specifies the server to test against
  * testConfigurationFiles: List of names of files in the resources/configs directory that specify the test
  configurations to include in this suite

The server specification file for the test suite overrides the server specification file for all test configurations
contained in the suite.

## Troubleshooting
* Check that the server specification file property of the test configuration points to the correct URL you want to test
against.

* Check that your IP address is included on the IP whitelist for the cluster you're testing against. The easiest way to
do this is to connect to the Non-Split Broad VPN, because the VPN IP addresses are already included on the IP whitelist
for the Jade dev cluster.

* Check that you are calling the correct version of Gradle (6.1.1 or higher). Use the Gradle wrapper in the sub-project
(`.gradlew`), not the one in the parent directory (`../.gradlew`).

#### Debug a test configuration or script
* The Gradle run task just calls the main method of the TestRunner class, with the test configuration file passed in as
the first argument. To debug, add a Run/Debug Configuration in IntelliJ that calls the same method with the appropriate
argument.

* To debug a test script without the test runner, for example to make sure the API calls are coded correctly, add a main
method that executes the setup/userjourney/cleanup steps. You can copy the main method in the TestScript base class and
paste it into the test script class you want to debug. Then change the method to call the constructor of the test script
class you want to debug. Run the test script main method in debug mode.

## Build out the testing infrastructure
All the Java code is in the src/main/java directory. The runner package contains most of the testing infrastructure code,
including the POJO classes used to specify a test configuration. The common.utils package contains code that may be useful for
both test scripts and the test runner.

Run the linters before putting up a PR.

`./gradlew spotlessApply`

`./gradlew spotbugsMain`
