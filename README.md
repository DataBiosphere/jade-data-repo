# jade-data-repo
The repo for the terra data repository built by the jade team.

See the DATABASE.md to set up the postgres database before you run jade.

## Build and Run

If you are making code changes, run:
`./gradlew check`

To run jade locally:
`./gradlew bootRun`

To have the code hot reload, enable automatic builds in intellij, go to:
`Preferences -> Build, Execution, Deployment -> Compiler`
and select `Build project automatically`

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

## Environment variables
    OAUTH_CLIENT_ID
    OAUTH_CLIENT_SECRET
