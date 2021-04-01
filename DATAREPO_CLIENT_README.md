# Terra Data Repo Client project
The tdrclient directory within the data repo code base contains an independent
gradle project.

## Build Targets
The project has two targets that are interesting to use.

### build
The build target generates the OpenAPI client code for using the swagger source from the
data repo code base. The code is generated into the `src/` directory. That allows the
normal gradle java plugin to easily run the compile, build and test.

### artifactoryPublish
The artifactoryPublish target generates the extra files (pom.xml and so forth) needed to
publish the client into the Broad Artifactory.

In order to publish, you must know the artifactory username and password. You supply
those to gradle by setting these environment variables:
- ARTIFACTORY_USER
- ARTIFACTORY_PASSWORD

## Using the targets

These targets must be run from the top level data repo directory. To run them:

```shell
ENABLE_SUBPROJECT_TASKS=1 ./gradlew :datarepo-client:build
```

## Testing

One way to test this library is to push it to your local maven repository. This command must be run
in the top level data repo directory.

```shell
ENABLE_SUBPROJECT_TASKS=1 ./gradlew :datarepo-client:publishToMavenLocal
```

Once it's published, you can add your local repo to your maven dependency sources in your test
project. To do this in Gradle, add this line to your `build.gradle`'s `repositories` declaration.

```
    mavenLocal()
```

You may also want to change the library version to a different version to ensure that you're not
getting a cached version. To do this, change `version` in the root `build.gradle`:

```
    version '1.46.0-localtest'
```


## Versioning
The version number for the client code is the same as the data repo version, and a new library is built
whenever the data repo version is changed. The artifact for this library is automatically built
and pushed to Broad local repo when a branch is merged to develop by the
"Publish to Artifactory" step in [this github action](.github/workflows/dev-image-update.yaml).

## Why is there junk in the tdrclient directory after a build?
The swagger codegen creates a pile of extraneous files. I don't know how to turn them off.
They are `.gitignore`d, so they should not end up in github.
