# Getting Started

These instructions assume you use MacOS, and that you are on the internal Broad
network or the VPN. If the VPN is not installed, follow the instructions
[at this link](https://broad.io/vpn).

> During this process, you will need your GitHub and Docker Hub username,
password, and personal access token for multiple steps, so make sure to have
those handy. If you don't have those yet, see the section below, otherwise you
can skip to [Request Required Access](#2-request-required-access).

## 1. Create a GitHub and Docker Hub account

GitHub is where the Broad stores our code and projects. Docker Hub allows the
development team to easily deploy software without having to install lots of
dependencies.

Sign up to these services with your **personal** email:
  * https://github.com/join
  * https://hub.docker.com/signup

Create a [personal access token](https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line)
so you can interact with GitHub on the command line.

## 2. Request Required Access

Ensure that you have access to the required team resources. If you
encounter a permission error, it is likely because you are missing appropriate
access.

- DataBiosphere: Join the `#github` Slack channel, click the lightning bolt in the
channel header, and select `Join DataBiosphere`.  Once you've been granted access
to DataBiosphere, ask a team member to add your github user to the
[DataBiosphere/jadeteam group](https://github.com/orgs/DataBiosphere/teams/jadeteam).
This will give you admin access to our repositories.
- Google Groups: Ask a team member for access to Google Groups including `jade-internal` and `dsde-engineering`.

## 3. Connect accounts

> Make sure 2-factor authentication (2FA) is activated on your
[Broad](https://broad.io/2fa) and [GitHub](https://github.com/settings/security)
account before starting this process!

Connect your GitHub account to your Broad profile:

1. Go to [Broad people](https://people.broadinstitute.org/me) and select the
**My Profile** tab.
2. Link your profile to GitHub by clicking under **Other Profiles**.
3. [Check if the account is successfully linked](https://github.broadinstitute.org/).
4. Open each of the following GitHub groups and **Request to join** by going to
the Members tab: [Broad Institute Read](https://github.com/orgs/broadinstitute/teams/broad-institute-read),
[Prometheus](https://github.com/orgs/broadinstitute/teams/prometheus),
[DSDE Engineering](https://github.com/orgs/broadinstitute/teams/dsde-engineering)
5. To avoid being overwhelmed with notifications, [add your Broad email address](https://github.com/settings/emails),
[route the notifications](https://github.com/settings/notifications) to that
email, and [unfollow projects](https://github.com/watching) that are not
relevant to your team.

Connect your Docker Hub account to your Broad profile by contacting the DevOps
team.

## 4. Create Terra Accounts

The Data Repo and [Terra](https://terra.bio/) use [Sam](https://github.com/broadinstitute/sam)
to abstract identity and access management. To gain access to these services,
first create a non-Broad email address through Gmail. This email address will
specifically be used for development purposes in our non-prod environments.

BITS requires that these development accounts have multi-factor authentication (MFA) enabled.
Follow Google's instructions for enabling
[two-step authentication](https://support.google.com/accounts/answer/185839).
When complete, document your development account with a screenshot showing that it has MFA enabled
[here](https://docs.google.com/document/d/1FS_DC1ysF861ZZhgGXNM-KB8_smalVeDjz1SxyOr9GM/edit#).

Next, to register as a new user, click the `Sign in with Google` button in each
of the environments with the newly created email address and follow the prompts:

- [Dev](https://bvdp-saturn-dev.appspot.com/)
- [Alpha](https://bvdp-saturn-alpha.appspot.com/)
- [Staging](https://bvdp-saturn-staging.appspot.com/)

For [production](https://app.terra.bio/), you will need to register using a
`firecloud.org` email. In order to get an account, you must become suitable,
which requires following [these steps](https://docs.google.com/document/d/1DRftlTe-9Q4H-R0jxanVojvyNn1IzbdIOhNKiIj9IpI/edit?usp=sharing).

Ask a member of the team to add you to the admins group for each of these
environments.

## 5. Install Homebrew

[Homebrew](https://brew.sh/) is a [package manager](https://en.wikipedia.org/wiki/Package_manager)
which enables the installation of software using a single, convenient command
line interface. To automatically install development tools necessary for the
team, a [Brewfile](https://github.com/Homebrew/homebrew-bundle) is used:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
curl -LO https://raw.githubusercontent.com/DataBiosphere/jade-data-repo/develop/docs/Brewfile
brew bundle --no-lock install
```

The Brewfile automatically installs the following tools:

1. [Git](https://git-scm.com/) is a version control tool for tracking changes in
projects and code.
2. [jq](https://stedolan.github.io/jq/) is a command line JSON processing tool.
3. [Docker](https://www.docker.com/) is a tool to deliver software in packages
called containers. Docker for MacOS also includes [Kubernetes](https://kubernetes.io/),
which deploys groups of containers together in clusters.
4. [Helm](https://helm.sh/) streamlines the process of defining, installing, and
upgrading Kubernetes deployments, which are otherwise challenging to manage.
Some manual configuration is required below.
5. [Helmfile](https://github.com/roboll/helmfile) streamlines deploying multiple
helm charts.
6. [Vault](https://www.vaultproject.io/) is an encrypted database used to store
many of the team's secrets such as keys and passwords.
7. [Google Cloud SDK](https://cloud.google.com/sdk) is a command-line interface
to Google Cloud services. Once it is installed, you'll need to allow auth access
and configure Docker to connect to the appropriate Google Cloud endpoint when
necessary, which is done with the configuration below.
8. [IntelliJ IDEA](https://www.jetbrains.com/idea/) is an integrated development
environment (IDE) for Java. There are two versions available: **Ultimate** (paid)
and **Community** (open-source). We recommend the Ultimate Edition to Broad
employees for its database navigation capabilities. Alternatively, the Community
Edition has all the features needed for development, and this version can be
installed by switching `intellij-idea` with `intellij-idea-ce` in the Brewfile.
9. [Skaffold](https://github.com/GoogleContainerTools/skaffold) is a command line
tool that facilitates continuous development for Kubernetes applications.  It is
used to test local changes against personal environments.

Unfortunately, some manual configuration is also necessary:

```
# configure vault
export VAULT_ADDR=https://clotho.broadinstitute.org:8200

# configure helm
helm repo add datarepo-helm https://broadinstitute.github.io/datarepo-helm
helm plugin install https://github.com/thomastaylor312/helm-namespace
helm plugin install https://github.com/databus23/helm-diff
helm repo update

# launch docker desktop - this installs docker in /usr/local/bin
open -a docker

# configure google-cloud-sdk
gcloud auth login
gcloud auth application-default login
gcloud auth configure-docker
```

## 6. Create GitHub token

The GitHub token verifies team permissions. This token is necessary for the next
step, [Login to Vault](#6-login-to-vault). To create a token:

1. Go to the [GitHub Personal Access Token](https://github.com/settings/tokens)
page and click **Generate new token**.
2. Give the token a descriptive name, **only** give it the following two scopes and then click **Generate token**.
  *  `read:org` scope under `admin:org`
  *  `workflow` (this will give you access to kick off github actions from the command line)
3. Store this token in a file:

```
GH_VAULT_TOKEN=<<GITHUB TOKEN VALUE>>
echo $GH_VAULT_TOKEN > ~/.gh_token
```

## 6. Login to Vault

Vault access tokens can be obtained using the GitHub token from earlier as
follows:

```
vault login -method=github token=$(cat ~/.gh_token)
```

## 7. Code Checkout

> It may be useful to create a folder for Broad projects in your home directory.

Setup [Github SSH](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh)

Download the team's projects:

```
git clone git@github.com:DataBiosphere/jade-data-repo.git
git clone git@github.com:DataBiosphere/jade-data-repo-ui.git
git clone git@github.com:DataBiosphere/jade-data-repo-cli.git
git clone git@github.com:broadinstitute/datarepo-helm.git
git clone git@github.com:broadinstitute/datarepo-helm-definitions.git
git clone git@github.com:broadinstitute/terra-helmfile.git
git clone git@github.com:broadinstitute/terraform-ap-deployments.git
git clone git@github.com:broadinstitute/terraform-jade.git
```

## 8. Set up your Development Environment

The goal of this step is set up some of the basic components of your development
environment.  You'll actually spin up this instance on broad-jade-dev in the next step.

> There is a video of us walking through
these steps in our [Jade Google Drive Folder](https://drive.google.com/drive/folders/1JM-_M0qsX6eXocyPc9TB7ivCKJTji3dX?usp=sharing).

1. Follow the
   [instructions in our terraform-jade repository](https://github.com/broadinstitute/terraform-jade/tree/master/old#new-team-member-process)
   to add your initials to the terraform templates and generate the static resources needed
   to deploy your personal development environment.
   Apply the changes and create a pull request to merge your additions to `terraform-jade`.

2. Create your datarepo helm definition:
  -  In `datarepo-helm-definitions/dev` directory,
     copy an existing developer definition and change all initials to your own.
     Double-check with the team if you're not sure what to use, but the most recently added
     is probably the best choice.
  -  By default, leave release chart versions unspecified in your `helmfile.yaml` so that
     latest versions are automatically picked up when running helmfile commands.
     Otherwise, verify that specified versions match the
     [latest dependency versions](https://github.com/broadinstitute/datarepo-helm/blob/master/charts/datarepo/Chart.lock).
  -  Create a pull request with these changes in
     [datarepo-helm-definitions](https://github.com/broadinstitute/datarepo-helm-definitions).

## 9. Google Cloud Platform setup

Throughout these instructions, replace all instances of `ZZ` with your initials.

1. Log in to [Google Cloud Platform](https://console.cloud.google.com).
   In the top-left corner, select the **BROADINSTITUTE.ORG** organization.
   Select **broad-jade-dev** from the list of projects.

2. From the left hand sidebar, select **Kubernetes Engine -> Clusters** under
   **COMPUTE**.

3. Click **Connect** on the **dev-master** cluster.
   (You can also navigate here via
   [direct link](https://console.cloud.google.com/kubernetes/clusters/details/us-central1/dev-master/details?project=broad-jade-dev).)
   This gives you a `kubectl` command to copy and paste into the terminal:

```
gcloud container clusters get-credentials dev-master --region us-central1 --project broad-jade-dev
```

4. Starting from your [project directory](#6-code-checkout) in `datarepo-helm-definitions`,
   bring up Helm services (note it will take up to 10-15 minutes for ingress and cert creation):

Note: Make sure you are on the VPN, otherwise the helmfile apply will fail.

```
cd datarepo-helm-definitions/dev/ZZ
helmfile apply

# check that the deployments were created
helm list --namespace ZZ
```

5. Update the following authorized domains within the
   [Jade Data Repository OAuth2 Client configuration](https://console.cloud.google.com/apis/credentials/oauthclient/970791974390-1581mjhtp2b3jmg4avhor1vabs13b7ur.apps.googleusercontent.com?authuser=0&project=broad-jade-dev):

 - Under Authorized JavaScript origins, add `https://jade-ZZ.datarepo-dev.broadinstitute.org`
 - Under Authorized redirect URIs, add `https://jade-ZZ.datarepo-dev.broadinstitute.org/login/google` and
   `https://jade-ZZ.datarepo-dev.broadinstitute.org/webjars/springfox-swagger-ui/oauth2-redirect.html`

6. Connect to your new dev postgres database instance:
   Note that this is a different instance than the local one you will configure in [step 10](#10-install-postgres-11).
   The following command connects to the database via a proxy.

```
cd jade-data-repo/ops
DB=datarepo-ZZ SUFFIX=ZZ ENVIRONMENT=dev ./db-connect.sh
```

7. Now that you're connected to your dev database, run the following command
   (Once [DR-1156](https://broadworkbench.atlassian.net/browse/DR-1156) is done, this will no longer be needed):

```
create extension pgcrypto;
```

8. Create a pull request to `terraform-ap-deployments` to add
  `https://jade-ZZ.datarepo-dev.broadinstitute.org` under the
  ['personal deployments'](https://github.com/broadinstitute/terraform-ap-deployments/blob/e9ecc7a637fe4a7743011b568f76a296a4e85ed2/azure/b2c/tfvars/dev.tfvars#L20)
  section of `dev.tfvars/b2c_tdr_hosts`.  This allows B2C as a means of authentication, which is
  the default across environments.

## 10. Install Postgres 11

[Postgres](https://www.postgresql.org/) is an advanced open-source database.
**Postgres.app** is used to manage a local installation of Postgres.
The latest release can be found on the [GitHub releases](https://github.com/PostgresApp/PostgresApp/releases)
page.
For compatibility, make sure to select a version which supports
all the older versions of Postgres including 9.6.
After launching the application, create a new version 11 database as follows:

1. Click the sidebar icon (bottom left-hand corner) and then click the plus sign
2. Name the new server, making sure to select version **11**, and then
**Initialize** it
3. Add `/Applications/Postgres.app/Contents/Versions/latest/bin` to your path
(there are multiple ways to achieve this)
4. Switch to the `jade-data-repo` repository, and create the data repo database
and user following the [database readme](https://github.com/DataBiosphere/jade-data-repo/blob/develop/DATABASE.md):

```
psql -f db/create-data-repo-db
# verify that the `datarepo` and `stairway` databases exist
psql --list
```

## 11. Configure Azure
You will need to have an Azure account created (see https://docs.google.com/spreadsheets/d/1Q6CldqVPrATkWCAXljKrwlLz8oFsCQwcfOz_io-gcrA)
and granted access to the TDR application in Azure and added to the jadedev group.

The Azure user should look like <your email id>@azure.dev.envs-terra.bio

Both are performed by a teammate in the Azure portal: https://portal.azure.com


## 12. Repository Setup

### 1. Build `jade-data-repo`

Follow the [Build and Run Locally](https://github.com/DataBiosphere/jade-data-repo#build-and-run-locally)
section in the [main readme](https://github.com/DataBiosphere/jade-data-repo#jade-data-repository---)
to build `jade-data-repo`.

* You will need to run `render-configs.sh` before running integration tests.

* **Set Environment Variables**: While not exhaustive,
  here's a list that notes the important environment variables to set when running `jade-data-repo` locally.
  Instances of `ZZ` should be replaced by your initials or the environment (i.e. `dev`).
  These variables override settings in jade-data-repo/application.properties.
  You can convert any application.property to an environment variable by switching to upper case and every "." to "_".

```
# Point to your personal dev project/deployment
export PROXY_URL=https://jade-ZZ.datarepo-dev.broadinstitute.org
export JADE_USER_EMAIL=<EMAIL_YOU_CREATED_FOR_DEVELOPMENT>

# Integration test setting: change this to http://localhost:8080/ to run against a local instance
export IT_JADE_API_URL=https://jade-ZZ.datarepo-dev.broadinstitute.org

# This file will be populated when you run ./render-configs.sh
export GOOGLE_APPLICATION_CREDENTIALS=/tmp/jade-dev-account.json
export GOOGLE_SA_CERT=/tmp/jade-dev-account.pem

# Clears database on startup, test run, etc. This is further explained in the oncall playbook.
export DB_MIGRATE_DROPALLONSTART=true

# Setting for testing environment (Further explaned in oncall playbook)
export GOOGLE_ALLOWREUSEEXISTINGBUCKETS=true

# Setting for credentials to test on Azure
export AZURE_CREDENTIALS_HOMETENANTID=$(cat /tmp/jade-dev-tenant-id.key)
export AZURE_CREDENTIALS_APPLICATIONID=$(cat /tmp/jade-dev-client-id.key)
export AZURE_CREDENTIALS_SECRET=$(cat /tmp/jade-dev-azure.key)
export AZURE_SYNAPSE_SQLADMINUSER=$(cat /tmp/jade-dev-synapse-admin-user.key)
export AZURE_SYNAPSE_SQLADMINPASSWORD=$(cat /tmp/jade-dev-synapse-admin-password.key)
export AZURE_SYNAPSE_ENCRIPTIONKEY=$(cat /tmp/jade-dev-synapse-encryption-key.key)
export AZURE_SYNAPSE_INITIALIZE=false
```

* If you're not on a **Broad-provided** computer, you may need to set the host to `localhost`
instead of `http://local.broadinstitute.org`:

```
export HOST=localhost
```

* Build the code and run the tests:

```
./gradlew bootRun         # build jade-data-repo with Spring Boot features
./gradlew check           # linters and unit tests
./gradlew testConnected   # connected tests
./gradlew testIntegration  # integration tests
```

Note that connected and integration test suites can each take 90+ minutes to run.
In normal development, you'll likely rely on GitHub Actions / automated PR test runs
to run all tests, initially running locally those tests which pertain to your work.

To run a subset of tests, you can specify `--tests <pattern>` when running
the above test commands.  More specific examples are available in
[Gradle documentation](https://docs.gradle.org/current/userguide/java_testing.html#test_filtering).

### 2. Build `jade-data-repo-ui`

Follow the [setup instructions](https://github.com/DataBiosphere/jade-data-repo-ui#jade-data-repository-ui)
to build the `jade-data-repo-ui` repository.

## 13. Developing Locally

After running bootRun, you may want to create some datasets locally for use in testing.
To do this, you can run this python
[script](https://github.com/DataBiosphere/jade-data-repo-ui/blob/develop/tools/ui_integration/setup_ui_integration.py)
against your local db.

You can also run some of the notebooks from [the Jade Client examples](https://github.com/broadinstitute/jade-data-repo-client-example/tree/master/src/main/python),
such as `AzureY1Demo.ipynb`

## Common Issues

Ensure that:

1. You are on the Broad Non-split VPN. See earlier [instructions](#-getting-started).
2. Docker is running.
3. Postgres database is started.
4. Environment variables are set. See list of environment variables [above](#12-repository-setup).
