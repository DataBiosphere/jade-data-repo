# jade-data-repo
The repo for the terra data repository built by the jade team.

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


Generate code from the swagger-codegen yml file
`swagger-codegen generate -i src/main/resources/data-repository-openapi.yaml -l spring` 