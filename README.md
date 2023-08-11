# [jade-data-repository](https://data.terra.bio/) &middot; [![GitHub license](https://img.shields.io/github/license/DataBiosphere/jade-data-repo)](https://github.com/DataBiosphere/jade-data-repo/blob/develop/LICENSE.md) [![Integration test and Connected tests](https://github.com/DataBiosphere/jade-data-repo/workflows/Integration%20test%20and%20Connected%20tests/badge.svg?branch=develop)](https://github.com/DataBiosphere/jade-data-repo/actions?query=workflow%3A%22Integration+test+and+Connected+tests%22+branch%3Adevelop)

The [Terra](https://terra.bio/) Data Repository built by the Jade team as part of the
[Data Biosphere](https://medium.com/@benedictpaten/a-data-biosphere-for-biomedical-research-d212bbfae95d).

* Support **complex, user-specified schemas** and different types of data
* **Fine-grained** access control
* Support **share-in-place**, copying data is expensive
* **Cloud-transparency:** support off-the-shelf tool access to data

## Documentation

This repository is currently designed to be deployed inside of a Google Cloud Platform project to manage tabular data
inside of BigQuery datasets and file data inside of Google Cloud Storage buckets. The project setup has been automated
via Terraform.

### Terraforming a project

Clone the [terraform-jade](https://github.com/broadinstitute/terraform-jade) repo and follow the terrform commands there
to set it up.

Note: those Terraform scripts and the deployment script here make an assumption that they can retrieve
secrets from a [Vault](https://www.vaultproject.io/) server at certain paths. If you are standing this repo outside
of the Broad infrastructure, the current best alternative is to set up a Vault server to supply these secrets until
we implement less opinionated way to supply secrets to the deployment scripts.

### Setting up access

Now that your cluster is Terraformed, you need to be able to access it with kubectl commands. To do this, go to the
Google Cloud Console -> Kubernetes Engine -> Clusters click the Connect button next to you cluster info. Copy the
command and execute it on your local system. Now, if you click on Docker -> Kubernetes you should see a check next to
the cluster you just created.

Your cluster will be running under the default compute service account for the project. This account is used by Google
Kubernetes Engine (GKE) to pull container images clusters by default. It is in the form
[PROJECT_NUMBER]-compute@developer.gserviceaccount.com, where [PROJECT-NUMBER] is the GCP project number of the project
that is running the Google Kubernetes Engine cluster.

Note: this next part is specific to the Broad setup. If you are standing this up externally, you will need an instance
of Google Container Registry (GCR) where you can put images to be deployed in GKE.

Give your service account access to dev GCR:

    gsutil iam ch serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com:objectViewer gs://artifacts.broad-jade-dev.appspot.com

### Using cloud code and skaffold

Once you have deployed to GKE, if you are developing on the API it might be useful to update the API container image
without having to go through a full re-deploy of the Kubernetes namespace. CloudCode for IntelliJ makes this simple.
First install [skaffold](https://github.com/GoogleContainerTools/skaffold):

    brew install skaffold

Next, [enable the CloudCode plugin for IntelliJ](https://cloud.google.com/code/docs/intellij/quickstart-IDEA).

Then you should be able to either `Deploy to Kubernetes` or `Develop on Kubernetes` from the run configurations menu.

## Build and Run Locally

### Set up
You must have authenticated with google for application-default credentials:

    gcloud auth application-default login

and login with an account that has access to your project. This will save credentials locally. If you are using
multiple accounts, you can switch to the correct one using this command:

    gcloud config set account <account email>

Then you must specify a google project to use. Run this command:

    gcloud config set project <project-name>

To see what you currently have set, use: `gcloud config list`

When running locally, we are not using the proxy. Therefore, the system doesn't know your user email. Edit the
`src/main/resources/application.properties` file and set the userEmail field. If you are running sam locally, set
`sam.basePath` to `https://local.broadinstitute.org:50443`.

### Run linters and unit tests

If you are making code changes, run:
`./gradlew check`

### Run jade locally

Before you run for the first time, you need to generate the credentials file by running `./render-configs.sh`

To run jade locally:
`./gradlew bootRun`

To run jade locally and wait for debugger to attach on port 5005:
`./gradlew bootRun --debug-jvm`

To have the code hot reload, enable automatic builds in intellij, go to:
`Preferences -> Build, Execution, Deployment -> Compiler`
and select `Build project automatically`

Note: when running locally, it may be useful to not log in JSON but as traditional log message.  This can be enabled by
setting the environment variable:
`TDR_LOG_APPENDER=Console-Standard`
(the default is "Console-Stackdriver")

The swagger page is:
https://local.broadinstitute.org:8080

### Run connected and integration tests
`./gradlew testConnected`

The integration tests will hit the data repo running in the  broad-jade-integration envrionment by default. To use a
different data-repo, edit the src/main/resources/application-integration.properties file and specify the URL. Before
you run the integration tests, you need to generate the correct pem file by running `./render-configs.sh`

To run the tests, use: `./gradlew testIntegration`

## Swagger Codegen (deprecated)

We are using swagger-codegen to generate code from the swagger (OpenAPI) document. Therefore, in order to build
you need to have the codegen tool installed from [swagger-codegen](https://swagger.io/docs/open-source-tools/swagger-codegen/).

The gradle compile uses swagger-codegen to generate the model and controller interface code into
`src/generated/java/bio/terra/models` and `src/generated/java/bio/terra/controllers` respectively. Code in `src/generated` is not committed to github. It is generated as needed.

Adding an endpoint to the API source (data-repository-openapi.yaml) will generate the endpoint definition in the
appropriate controller interface file. Swagger-codegen provides a default implementation of the endpoint that generates
a NOT_IMPLEMENTED return. You add the actual implementation of the new interface by editing the Jade controller code
in `src/main/java/bio/terra/controller`. That overrides the default interface implementation.

Clearly, you can make breaking changes to the API and will have to do the appropriate refactoring in the rest of
the code base. For simple addition of fields in a structure or new endpoints, the build will continue to run clean.

In the rare case of wanting to have swagger-codegen create a controller class,
in a directory other than a git cloned workspace, run:
`swagger-codegen generate -i path/to/data-repository-openapi.yaml -l spring -c path/to/config.json`

Then copy the files you want into the source tree

## skaffold
To render your own local skaffold.yaml run the following with your initials
```
sed -e 's/TEMP/<initials>/g' skaffold.yaml.template > skaffold.yaml
```
Run a deployment you must set env var `IMAGE_TAG`
```
skaffold run
```

## Add new application property
1. Locally, application properties are controlled by the values in the various application.properties files.
    - `application.properties` contains the base/default values. A new property should be added here first.
    ```
    google.allowReuseExistingBuckets=false
    ```
    - You can override the default value for connected and integration tests by adding a line to
    `application-connectedtest.properties` and `application-integrationtest.properties`.
    ```
    google.allowReuseExistingBuckets=true
    ```
2. Now that we use Helm, the properties also need to be added to the
[base Data Repo charts](https://github.com/broadinstitute/datarepo-helm).
    - Find the the [api-deployment.yaml](https://github.com/broadinstitute/datarepo-helm/blob/master/charts/datarepo-api/templates/api-deployment.yaml) file.
    - Add a new property under the `env` section. The formatting below might be messed up, and the yaml is very picky
    about spaces. So, copy/paste from another variable in the section instead of here.
    ```
            {{- if .Values.env.googleAllowreuseexistingbuckets }}
            - name: GOOGLE_ALLOWREUSEEXISTINGBUCKETS
              value: {{ .Values.env.googleAllowreuseexistingbuckets | quote }}
            {{- end }}
    ```
    - Find the the [values.yaml](https://github.com/broadinstitute/datarepo-helm/blob/master/charts/datarepo-api/values.yaml) file.
    - Add a new line under the `env` section.
    ```
      googleAllowreuseexistingbuckets:
    ```
    - Release a new version of the chart. Talk to DevOps to do this.
3. To override properties for specific environments (e.g. integration), modify the
[environment-specific override Data Repo charts](https://github.com/broadinstitute/datarepo-helm-definitions).
    - Find the [deployment.yaml](https://github.com/broadinstitute/datarepo-helm-definitions/blob/master/integration/integration-1/integration-1Deployment.yaml)
    for the specific environment.
    - Add a new line under the `env` section.
    ```
    googleAllowreuseexistingbuckets: true
    ```
   - It's a good idea to test out changes on your developer-namespace before making a PR.
   - Changes to integration, temp, or developer-namespace environments are good with regular PR approval (1 thumb for this repository).
   - Changes to dev or prod need more eyes, and perhaps a group discussion to discuss possible side effects or failure modes.

## Developer Notes
### Proper Handling of InterruptedException
Care must be taken with a handling of InterruptedException. Code running in stairway flights must expect to receive
InterruptedException during waiting states when the pod is being shut down. It is important that exception be allowed to
propagate up to the stairway layer, so that proper termination and re-queuing of the flight can be performed.

On the other hand, code running outside of the stairway flight, where the exception can become the response to
a REST API, should catch the InterruptedException and replace it with a meaningful exception. Otherwise the caller
 gets a useless error message.

## Deployments
The deployments of Terra Data Repository are:
- [Production](https://data.terra.bio/)
- [Development](https://jade.datarepo-dev.broadinstitute.org/)
- [Development - swagger page](https://jade.datarepo-dev.broadinstitute.org/swagger-ui.html)

### Running Sonar locally

[Sonar](https://www.sonarqube.org) is a static analysis code that scans code for a wide
range of issues, including maintainability and possible bugs. If you get a build failure due to
SonarQube and want to debug the problem locally, you need to get the sonar token from vault
before running the gradle task.

```shell
export SONAR_TOKEN=$(vault read -field=sonar_token secret/secops/ci/sonarcloud/data-repo)
./gradlew sonar
```

Running this task produces no output unless your project has errors. To always
generate a report, run using `--info`:

```shell
./gradlew sonar --info
```
