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
those to the gradle build by setting these environment variables:
- ARTIFACTORY_USER
- ARTIFACTORY_PASSWORD

## Versioning
The version number for the client code is **independent** of the of the data repo version.
Right now, it is always a SNAPSHOT version. I suggest that for now we bump the
patch version with every backward compatible change made to the API. We bump the
minor version when we make an incompatible change. And we keep the major version matching
the `v1` in the paths from the OpenAPI.

Until we figure out something more clever, you must manually bump the version number
by editing the gradle.build file.

## Why is there junk in the tdrclient directory after a build?
The swagger codegen creates a pile of extraneous files. I don't know how to turn them off.
They are `.gitignore`d, so they should not end up in github.
