## Infrastructure

### [General Deployment Process Overview Doc](https://github.com/broadinstitute/dsp-devops-wiki/wiki/deployment_process)

### Infrastructure Overview
The Google Infrastructure is managed via Terraform and nearly all the kubenetes deployments and such are done via a Helm Chart. See [General Deployment Process Overview Doc](https://github.com/broadinstitute/dsp-devops-wiki/wiki/deployment_process) for more in depth guide. Datarepo is deployed on Kubernetes, Google Cloud Sql (postgres) and contains the following containers:

- api (datarepo api/DRmanager container)
- ui (datarepo ui container)
- open id connect proxy (externally exposed endpoint proxy)
- google cloud sql proxy (sql proxy to database)

Container monitoring is done by [promethues operator](https://github.com/helm/charts/tree/master/stable/prometheus-operator) this includes:

- alertmanager (alertmanager can group send alerts)
- grafana (graphical ui charts/graphs)
- state-metric (collects state metrics)
- node-exporter (collects k8 node metrics)
- prometheus (backend for metrics)

Continuous deployment is managed by [Argocd](https://github.com/argoproj/argo-helm/tree/master/charts/argo-cd) this includes:
- dex (sso authentication container)
- argo-repo-server (repo server)
- argo-server (ui server)
- redis (in-memory data structure store)
- argo-application-controller

### Helm Overview
#### Repos
- [Datarepo-helm](https://github.com/broadinstitute/datarepo-helm)
  - Contains datarepo specific charts api, ui, proxy
- [monster-helm](https://github.com/broadinstitute/monster-helm)
  - Contains gcp managed cert chart
- [argo-helm](https://github.com/argoproj/argo-helm/tree/master/charts/argo-cd)
  - Contains argocd chart for continuous deployment
- [stable/prometheus-operator](https://github.com/helm/charts/tree/master/stable/prometheus-operator)
  - Contains helm stable which includes the prometheus-operator chart

#### Definitions
This is specifically referring to a helm `Values.yaml` file. This file contains all the user specific values to be overritten in the chart being used. Datarepo uses an umbrella chart and each users values can be found [here](https://github.com/broadinstitute/datarepo-helm-definitions)

## Github Actions
GitHub Actions automates all your workflows and jobs that happen in the background. Here we will cover datarepo specific actions. These are mostly for internal use such as releasing charts and bumping deployments. This doc is going to assume you understand the basics [here](https://help.github.com/en/actions).

## Action Syntax
Some actions can be defined within a yaml passing bash to it or it can be abstracted to a [container](https://help.github.com/en/actions/building-actions/creating-a-docker-container-action) that is build with pre written code to be acted on.

- [Jade backend container action repo](https://github.com/broadinstitute/datarepo-actions)

#### Datarepo Actions
- [Jade-Datarepo integration testing](https://github.com/DataBiosphere/jade-data-repo/blob/develop/.github/workflows/gradle-build-pr.yml)
  - On pull request
  - This action has two blocks that will spawn two jobs one is the connected test and the other is the integration test. This will take the code from a PR and build a container then test it.
- [Jade-Datarepo dev container build and helm definition bump](https://github.com/DataBiosphere/jade-data-repo/blob/develop/.github/workflows/dev-image-update.yaml)
  - On Merge to develop
  -  This action takes the passed code from [Jade-Datarepo integration testing](https://github.com/DataBiosphere/jade-data-repo/blob/develop/.github/workflows/gradle-build-pr.yml) and builds a develop approved container specifically for the develop branch meaning the built container has passed integration tests and now has a tag containing the "commit hash+-develop". Once the container is build it will checkout the [datarepo-helm-definitions](https://github.com/broadinstitute/datarepo-helm-definitions) and increment the api version [tag shown here](https://github.com/broadinstitute/datarepo-helm-definitions/commit/80c0abd317981970cf979498895ab43171d8f544)
- [Jade-Datarepo-ui integration testing](https://github.com/DataBiosphere/jade-data-repo-ui/blob/develop/.github/workflows/test-e2e.yml)
    - On pull request
    - This action will start a job one, the UI end to end tests. This will take the code from a PR and build a container then test it.
- [Jade-Datarepo-ui dev container build and helm definition bump](https://github.com/DataBiosphere/jade-data-repo-ui/blob/develop/.github/workflows/dev-image-update.yaml)
  - On Merge to develop
  - This action takes the passed code from Jade-Datarepo-ui integration testing and builds a develop approved container specifically for the develop branch meaning the built container has passed integration tests and now has a tag containing the "commit hash+-develop". Once the container is build it will checkout the datarepo-helm-definitions repo and bump the ui version tag in the dev values.

#### Helm Actions
- [Helm lint and simple test](https://github.com/broadinstitute/datarepo-helm/blob/master/.github/workflows/lint.yaml)
  - on pull request
  - lints helm charts
- [Chart Releaser](https://github.com/broadinstitute/datarepo-helm/blob/master/.github/workflows/cr.yaml)
  - on merge to master
  - Searches for new version bumps in `Chart.yaml` within all the charts then cuts a github release zip/tarball
  - Once github release is cut it then checks out gh-pages and updates the `index.yaml` for the helm repository
- [Helm Definitions linter](https://github.com/broadinstitute/datarepo-helm-definitions/blob/master/.github/workflows/lint.yaml)
  - on pull request
  - Simple yamllint some where between relaxed and strict using [custom config](https://github.com/broadinstitute/datarepo-helm-definitions/blob/master/.github/yamllint.yaml)
