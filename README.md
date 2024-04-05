# [jade-data-repository](https://data.terra.bio/) &middot; [![GitHub license](https://img.shields.io/github/license/DataBiosphere/jade-data-repo)](https://github.com/DataBiosphere/jade-data-repo/blob/develop/LICENSE.md) [![Integration test and Connected tests](https://github.com/DataBiosphere/jade-data-repo/workflows/Integration%20test%20and%20Connected%20tests/badge.svg?branch=develop)](https://github.com/DataBiosphere/jade-data-repo/actions?query=workflow%3A%22Integration+test+and+Connected+tests%22+branch%3Adevelop)

The [Terra](https://terra.bio/) Data Repository built by the Jade team as part of the
[Data Biosphere](https://medium.com/@benedictpaten/a-data-biosphere-for-biomedical-research-d212bbfae95d).

* Support **complex, user-specified schemas** and different types of data
* **Fine-grained** access control
* Support **share-in-place**, copying data is expensive
* **Cloud-transparency:** support off-the-shelf tool access to data

More information can be found in our [terra support documentation](https://support.terra.bio/hc/en-us/sections/4407099323675-Terra-Data-Repository).

## Documentation

This repository is currently designed to be deployed inside of a Google Cloud Platform project to manage
tabular and file data backed either by GCP or Azure. The project setup has been automated via Terraform.

## Build and Run Locally

Follow our getting [started guide](docs/jade-getting-started.md) to get set up.

### Run linters and unit tests

If you are making code changes, run:
`./gradlew check`

### Verify Pact contracts

To verify that TDR adheres to the contracts published by its consumers, run:
```
./src/test/render-pact-configs.sh
# Reload your environment variables, e.g. src ~/.zshrc
./gradlew verifyPacts     # verify contracts published with TDR as the provider
```

By default, this will fetch published contracts from the live Pact broker.
Results of Pact verification are only published when running in a CI environment (not locally).

### Run TDR locally

Before you run for the first time, you need to generate the credentials file by running `./render-configs.sh`

To run TDR locally:
`./gradlew bootRun`

To run TDR locally and wait for debugger to attach on port 5005:
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

The integration tests will hit the data repo running in the  broad-jade-integration environment by default. To use a
different data-repo, edit the src/main/resources/application-integration.properties file and specify the URL. Before
you run the integration tests, you need to generate the correct pem file by running `./render-configs.sh`

To run the tests, use: `./gradlew testIntegration`


### SourceClear

[SourceClear](https://srcclr.github.io) is a static analysis tool that scans a project's Java
dependencies for known vulnerabilities. If you are working on addressing dependency vulnerabilities
in response to a SourceClear finding, you may want to run a scan off of a feature branch and/or local code.

#### Github Action

You can trigger TDR's SCA scan on demand via its
[Github Action](https://github.com/broadinstitute/dsp-appsec-sourceclear-github-actions/actions/workflows/z-manual-jade-data-repo.yml),
and optionally specify a Github ref (branch, tag, or SHA) to check out from the repo to scan.  By default,
the scan is run off of TDR's `develop` branch.

High-level results are outputted in the Github Actions run.

#### Running Locally

You will need to get the API token from Vault before running the Gradle `srcclr` task.

```sh
export SRCCLR_API_TOKEN=$(vault read -field=api_token secret/secops/ci/srcclr/gradle-agent)
./gradlew srcclr
```

High-level results are outputted to the terminal.

#### Veracode

Full results including dependency graphs are uploaded to
[Veracode](https://sca.analysiscenter.veracode.com/workspaces/jppForw/projects/106675/issues)
(if running off of a feature branch, navigate to Project Details > Selected Branch > Change to select your feature branch).
You can request a Veracode account to view full results from #dsp-infosec-champions.


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
