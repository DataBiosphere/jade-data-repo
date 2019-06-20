# jade-data-repo
The repo for the terra data repository built by the jade team.

See the DATABASE.md to set up the postgres database before you run jade.

## Terraform kubernetes cluster

Clone the [terrafor-jade](https://github.com/broadinstitute/terraform-jade) repo and follow the terrform commands there to set it up.

Once your cluster has been created, go to the google cloud console->Kubernetes->Clusters click the Connect button next to you cluster info. Copy the command and execute it on your local system. Now, if you click on docker -> kubernetes you should see a check next to the cluster you just created.


## Deploying to kubernetes


### Deploying in your own test account (not dev, integration, etc)

When a k8s cluster gets terraformed it will be running under the default compute service account for the project. This account is used by Google Kubernetes Engine to pull container images clusters by default. It is in the form [PROJECT_NUMBER]-compute@developer.gserviceaccount.com, where [PROJECT-NUMBER] is the GCP project number of the project that is running the Google Kubernetes Engine cluster.

Give your service account access to dev GCR:

    gsutil iam ch serviceAccount:[PROJECT_NUMBER]-compute@developer.gserviceaccount.com:objectViewer gs://artifacts.broad-jade-dev.appspot.com

Give you user admin access:
    
    kubectl create clusterrolebinding <username>-cluster-admin-binding --clusterrole cluster-admin --user <username>@broadinstitute.org

#### Environment variables
    GOOGLE_CLOUD_PROJECT
    ENVIRONMENT (local, dev)


Deploy:

    ./ops/deploy.sh

After you deploy, go to Kubernetes in the google cloud console, select services, and then add the IP address of the oidc-proxy-service to your /etc/hosts file as `local.datarepo-dev.broadinstitute.org`

## Build and Run Locally

### Set up
You must have authenticated with google for application-default credentials:

	gcloud auth application-default login
and login with an account that has access to your project. This will save credentials locally. If you are using multiple accounts, you can switch to the correct one using this command:

    gcloud config set account <account email>

Then you must specify a google project to use. Run this command:


    gcloud config set project <project-name>


To see what you currently have set, use: `gcloud config list`

When running locally, we are not using the proxy. Therefore, the system doesn't know your user email. Edit the `src/main/resources/application.properties` file and set the userEmail field. If you are running sam locally, set `sam.basePath` to `https://local.broadinstitute.org:50443`.

### Environment variables

There are some secrets that need to be provided to the app and will not be checked in
to github. If you are standing this up on your own, you will need to set the following environment variables to the values [here](https://console.cloud.google.com/apis/credentials/oauthclient/970791974390-1581mjhtp2b3jmg4avhor1vabs13b7ur.apps.googleusercontent.com?project=broad-jade-dev&organizationId=548622027621)

    OAUTH_CLIENT_ID
    OAUTH_CLIENT_SECRET

### Run unit tests

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

## Swagger Codegen

We are using swagger-codegen to generate code from the swagger (OpenAPI) document. Therefore, in order to build
you need to have the codegen tool installed from [swagger-codegen](https://swagger.io/docs/open-source-tools/swagger-codegen/).

The gradle compile uses swagger-codegen to generate the model and controller interface code into
`src/generated/java/bio/terra/models` and `src/generated/java/bio/terra/controllers` respectively. Code in
`src/generated` is not committed to github. It is generated as needed.

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

