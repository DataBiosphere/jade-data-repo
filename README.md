# [jade-data-repository](https://jade-terra.datarepo-prod.broadinstitute.org/) &middot; [![GitHub license](https://img.shields.io/github/license/DataBiosphere/jade-data-repo)](https://github.com/DataBiosphere/jade-data-repo/blob/develop/LICENSE.md) [![TravisCI](https://travis-ci.org/DataBiosphere/jade-data-repo.svg?branch=develop)](https://travis-ci.org/DataBiosphere/jade-data-repo)

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

### Deploy to your Google project

The deployment script expects a few environment variables to be set:
- GOOGLE_CLOUD_PROJECT (broad-jade-initials)
- ENVIRONMENT (dev)
- SUFFIX (initials)

So to deploy to my broad-jade-jh project, I would run:

    GOOGLE_CLOUD_PROJECT=broad-jade-jh ENVIRONMENT=dev SUFFIX=jh ./ops/deploy.sh

Again, this deployment script is set to pull secrets out of Vault, so it uses the ENVIRONMENT and SUFFIX variables in
order to construct the right paths for lookups. Once you have deployed, you should have a set of pods and services
running inside of Kubernetes that are exposed via a Load Balancer and is accessible to the web.

It is useful to have the [jade-data-repo-ui](https://github.com/DataBiosphere/jade-data-repo-ui) repository checked out
next to this one, as the deployment script will automatically deploy the UI if it sees the directory.

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

To run jade locally:
`./gradlew bootRun`

To run jade locally and wait for debugger to attach on port 5005:
`./gradlew bootRun --debug-jvm`

To have the code hot reload, enable automatic builds in intellij, go to:
`Preferences -> Build, Execution, Deployment -> Compiler`
and select `Build project automatically`

The swagger page is:
https://local.broadinstitue.org:8080

### Run connected and integration tests
`./gradlew testConnected`

The integration tests will hit the data repo running in the  broad-jade-integration envrionment by default. To use a
different data-repo, edit the src/main/resources/application-integration.properties file and specify the URL. Before
you run the integration tests, you need to generate the correct pem file by running `./render_configs`

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
