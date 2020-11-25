# datarepo-clienttests
This Gradle project contains Test Runner tests written with the Data Repository client library.

The Test Runner library [GitHub repository](https://github.com/DataBiosphere/terra-test-runner) has documentation for
how to write and execute tests.

#### Run against a local server
There are localhost.json server specification files in the resources/server directory. These files contain a filepath to
the top-level directory of the jade-data-repo Git repository. Executing a test against this server, will start a local
Data Repo server by executing the Gradle bootRun task from that directory. This is useful for debugging or testing local
 server code changes.

You need to modify the path for your own machine. See deploymentScript.parameters below.

```json
{
  "name": "localhost",
  "description": "Server running locally. Supports launching the server in a separate process. Does not support modifying Kubernetes post-deployment.",
  "datarepoUri": "http://localhost:8080/",
  "samUri": "https://sam.dsde-dev.broadinstitute.org",
  "samResourceIdForDatarepo": "broad-jade-dev",
  "deploymentScript": {
    "name": "LaunchLocalProcess",
    "parameters": ["file:///Users/marikomedlock/Workspaces/jade-data-repo/"]
  },
  "skipDeployment": false,
  "skipKubernetes": true
}
```

#### Use a local Data Repo client JAR file
The version of the Data Repo client JAR file is specified in the build.gradle file for this project. This JAR file is
fetched from the Broad Institute Maven repository. You can override this to use a local version of the Data Repo client
JAR file by specifying a Gradle project property, either with a command line argument

`./gradlew -Pdatarepoclientjar=/Users/marikomedlock/Workspaces/jade-data-repo/datarepo-client/build/libs/datarepo-client-1.0.39-SNAPSHOT.jar run --args="configs/BasicUnauthenticated.json`

or an environment variable.

```
export ORG_GRADLE_PROJECT_datarepoclientjar=../datarepo-client/build/libs/datarepo-client-1.0.39-SNAPSHOT.jar
./gradlew runTest --args="configs/BasicUnauthenticated.json /tmp/TestRunnerResults"
```

This is useful for debugging or testing local server code changes that affect the generated client library (e.g. new API
endpoint). You can generate the Data Repo client library with the Gradle assemble task of the datarepo-client sub-project.

```
cd /Users/marikomedlock/Workspaces/jade-data-repo/datarepo-client
../gradlew clean assemble
ls -la ./build/libs/*jar
```
