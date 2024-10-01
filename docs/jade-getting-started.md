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
channel header, and select `Join DataBiosphere`. Once you've been granted access to DataBiosphere,
you should have write access to our repositories via membership in the
[DataBiosphere/broadwrite team](https://github.com/orgs/DataBiosphere/teams/broadwrite).
This level of permission should be sufficient for most contributions from across DSP.
  - If needed, repository admin access is conferred via membership in the
  [DataBiosphere/data-custodian-journeys team](https://github.com/orgs/DataBiosphere/teams/data-custodian-journeys),
  among others.
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
3. [Helm](https://helm.sh/) streamlines the process of defining, installing, and
upgrading Kubernetes deployments, which are otherwise challenging to manage.
Some manual configuration is required below.
4. [Helmfile](https://github.com/roboll/helmfile) streamlines deploying multiple
helm charts.
5. [Google Cloud SDK](https://cloud.google.com/sdk) is a command-line interface
to Google Cloud services. Once it is installed, you'll need to allow auth access
and configure Docker to connect to the appropriate Google Cloud endpoint when
necessary, which is done with the configuration below.
6. [IntelliJ IDEA](https://www.jetbrains.com/idea/) is an integrated development
environment (IDE) for Java. There are two versions available: **Ultimate** (paid)
and **Community** (open-source). We recommend the Ultimate Edition to Broad
employees for its database navigation capabilities (Please reach out to a team member
for the Broad server license address). Alternatively, the Community
Edition has all the features needed for development, and this version can be
installed by switching `intellij-idea` with `intellij-idea-ce` in the Brewfile.
7. [Skaffold](https://github.com/GoogleContainerTools/skaffold) is a command line
tool that facilitates continuous development for Kubernetes applications.  It is
used to test local changes against personal environments.

Unfortunately, some manual configuration is also necessary:

```
# configure helm
helm repo add datarepo-helm https://broadinstitute.github.io/datarepo-helm
helm plugin install https://github.com/thomastaylor312/helm-namespace
helm plugin install https://github.com/databus23/helm-diff
helm repo update

# launch docker desktop - this installs docker in /usr/local/bin
open -a docker

# configure google-cloud-sdk
# login with an account that has access to your project. This will save credentials locally.
gcloud auth login
gcloud auth application-default login

#If you are using multiple accounts, you can switch to the correct one using this command:
gcloud config set account <account email>

gcloud auth configure-docker

# setup kubectl plugin
gcloud components install gke-gcloud-auth-plugin
```

## 6. Code Checkout

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

## 7. Google Cloud Platform setup

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

## 8. Configure Azure

### 1. Get Azure Account
You will need to have an Azure account created (see https://docs.google.com/spreadsheets/d/1Q6CldqVPrATkWCAXljKrwlLz8oFsCQwcfOz_io-gcrA)
and granted access to the TDR application in Azure and added to the jadedev group.

The Azure user should look like <your email id>@azure.dev.envs-terra.bio

Both are performed by a teammate in the Azure portal: https://portal.azure.com

### 2. Create your own managed application in Azure
You must have your own managed application in order to create a TDR azure billing profile.
Create a "tdr-dev" managed application:
* Azure portal -> Marketplace -> "My Marketplace" -> "Private plans" -> There you should see the "tdr-dev" plan.
* Create a new tdr-dev plan with the following setup:
  * Subscription: 8201558_TDR_testuser1 (if you don't have access, ask for help from team)
  * Resource group: TDR
  * Application Name: <your name>
  * Hit "next"
* On the next screen, ***pay attention to the email you set in this field. It will be the email
you must log in as in order to create a TDR billing profile***. It should be a gmail account.
* Hit create!

## 9. Setup Environment Variable
There are several ways to go about this, but here is one way that works. You can set up a Z-shell
configuration to keep your system environment variables. If you don't already have one created, you
can create one by running `touch ~/.zshrc`. Then, you can open the file in a text editor with `open ~/.zshrc`. When you run
`./scripts/render-configs.sh`, it populates key and txt files with secrets from Google Cloud Secrets and environment-specific
values. If you are using the setup script, `./scripts/render-configs.sh` should automatically run.
An alternate is to run `./scripts/render-configs.sh -i` which will put the variables into your clipboard. You
can then paste these values into an intellij bootRun or test run profile.

### Environment Variables

While not exhaustive, here's a list that notes the important environment variables to set when running
`jade-data-repo` locally that are not set by `./scripts/render-configs.sh`. These variables override settings in jade-data-repo/application.properties.
You can convert any application.property to an environment variable by switching to upper case and
every "." to "_".

* Instances of `ZZ` are only needed if you have a personal development environment setup. It is no longer
  recommended to set this up. But, if used, `ZZ` should be replaced by your initials or the environment (i.e. `dev`).

```
export JADE_USER_EMAIL=<EMAIL_YOU_CREATED_FOR_DEVELOPMENT>

export AZURE_SYNAPSE_INITIALIZE=false

# Pact contract test settings
export PACT_BROKER_USERNAME=$(cat /tmp/pact-ro-username.key)
export PACT_BROKER_PASSWORD=$(cat /tmp/pact-ro-password.key)

# Setting for testing environment (Further explained in oncall playbook)
export GOOGLE_ALLOWREUSEEXISTINGBUCKETS=true

# If you're not on a **Broad-provided** computer, you may need to set the host to `localhost`
# instead of `http://local.broadinstitute.org`:
export HOST=localhost
```

## 10. Repository Setup

### 1. Build, run and Unit Test `jade-data-repo`

* Ensure docker is running
* Auth as your broadinstitute.org to pull from Google Secrets Manager `gcloud auth login <you>@broadinstitute.org`
* Run `./scripts/run-db start` to start the DB in a docker container
* Run `./scripts/run local` to run TDR locally or `./scripts/run docker` to run TDR in a docker container
* To Build the code and run the unit tests:

```
./scripts/build project   # build jade-data-repo and run unit tests
./scripts/run tests       # linters and unit tests
```

### 2. Run connected tests
We don't recommend running the entire connected test suite locally, as it takes over an hour to run.
Instead, you can select a specific test to run either in Intellij or the command line.
First, make sure you have run through the following steps:
* Ensure docker is running
* Auth as your broadinstitute.org to pull from Google Secrets Manager `gcloud auth login <you>@broadinstitute.org`
* Run `./scripts/run-db start` to start the DB in a docker container

**Run test in the Command Line**
* Run `GRADLE_ARGS='--tests *<specific test name>' ./scripts/run connected` to run a specific connected test

**Run or Debug test in Intellij**
* Run
`./scripts/render-configs.sh -i` which will put all the environment variables into your clipboard
and then you can paste them into the Intellij test setup.
* Select test in intellij UI, select 'testConnected' and run or debug it

### 3. Run Integration tests
We don't recommend running the entire integrated test suite locally, as it takes an hour to run.
Instead, you can select a specific test to run either in Intellij or the command line.
First, make sure you have run through the following steps:
* Ensure docker is running
* Auth as your broadinstitute.org to pull from Google Secrets Manager `gcloud auth login <you>@broadinstitute.org`
* Run `./scripts/run-db start` to start the DB in a docker container

**Run test in the Command Line**
* Run `GRADLE_ARGS='--tests *<specific test name>' ./scripts/run integration` to run a specific integration test

**Run or Debug test in Intellij**
* Run
  `./scripts/render-configs.sh -i -a integration` which will put all the environment variables into your clipboard and then you
  can paste them into the Intellij test setup.
* Start application by running `./scripts/run local` (or in docker with `./scripts/run docker`)
* Select test in intellij UI, select 'testIntegration' and run or debug it


### 4. Running Pact tests
This can be achieved by rendering a small set of Pact-specific configurations first:
```
./src/test/render-pact-configs.sh
# Reload your environment variables, e.g. src ~/.zshrc
./gradlew verifyPacts     # verify contracts published with TDR as the provider
```

Note that connected and integration test suites can each take 90+ minutes to run.
In normal development, you'll likely rely on GitHub Actions / automated PR test runs
to run all tests, initially running locally those tests which pertain to your work.

To run a subset of tests, you can specify `--tests <pattern>` when running
the above test commands.  More specific examples are available in
[Gradle documentation](https://docs.gradle.org/current/userguide/java_testing.html#test_filtering).

### 5. Build `jade-data-repo-ui`

Follow the [setup instructions](https://github.com/DataBiosphere/jade-data-repo-ui#jade-data-repository-ui) to build the `jade-data-repo-ui` repository.

By setting the `PROXY_URL` environment variable, you can point the UI to your local data repo instance.
```
export PROXY_URL=http://localhost:8080
```
You need to have data repo running with `./gradlew bootRun` and the UI running with `npm start`.

### 6. Testing in a deployed environment
**Testing in a BEE (Branch Engineering Environment)**
* You can test your changes in a BEE by following the instructions [here](https://docs.google.com/document/d/1kyjrOKzUthwKu-m38Da2niNEh-IkbUzxtfT29EWw8ag/edit?usp=sharing)
* You can point the [python setup script](https://github.com/DataBiosphere/jade-data-repo/blob/develop/tools/setupResourceScripts/setup_tdr_resources.py) to your BEE by setting the --host flag to the BEE url.

**Testing Helm Chart Changes (holdover until datarepo-helm moves to terra-helmfile)**
* Helm chart changes in datarepo-helm can be tested by spinning up a personal dev environment. See [instructions in datarepo-helm-definitions](https://github.com/broadinstitute/datarepo-helm-definitions) for more information.

## 11. Set up TDR resources

After running bootRun, you may want to create some datasets locally for use in testing.
To do this, you can point the [python setup script](https://github.com/DataBiosphere/jade-data-repo/blob/develop/tools/setupResourceScripts/setup_tdr_resources.py)
to your locally running data repo instance by setting the --host flag to http://localhost:8080.
See the [README](https://github.com/DataBiosphere/jade-data-repo/blob/develop/tools/setupResourceScripts/README.md) for more information.

You can also run some of the notebooks from [the Jade Client examples](https://github.com/broadinstitute/jade-data-repo-client-example/tree/master/src/main/python),
such as `AzureY1Demo.ipynb`

## 13. Running locally with other locally running services
1. Sam - set environment variable `SAM_BASEPATH` to `https://local.broadinstitute.org:50443`

## Common Issues

Ensure that:

1. You are on the Broad Non-split VPN. See earlier [instructions](#-getting-started). (Note: This is not needed for most operations)
2. Docker is running.
3. Postgres database is started.
4. Authed as your broadinstitute.org account
5. Environment variables are set. See list of environment variables [above](#12-repository-setup).
6. Ensure `./scripts/render-configs.sh` has been run and sourced to the command line
7. **Set Java Version in Intellij**: You may need to manually set the java version in Intellij for the jade-data-repo
     project.
  * File -> Project Structure -> Project -> SDKs -> add SDK -> Download JDK -> Version: 17, Vendor - AdoptOpenJDK 17 ( I used Termurin)
     ![image](https://github.com/DataBiosphere/jade-data-repo/assets/13254229/a1e7fe17-92ba-4e17-bf3b-523afe61e099)
     ![image](https://github.com/DataBiosphere/jade-data-repo/assets/13254229/d55f9883-0997-4b1f-979f-f011e78cec58)
  * You can also make sure this is correctly set under Intellij IDEA -> Preferences -> Build, Execution, Deployment -> Gradle -> Gradle JVM
   ![image](https://github.com/DataBiosphere/jade-data-repo/assets/13254229/e25bc825-3c3c-4ce0-9f1c-bed603db12f6)
8. `TERRA_COMMON_STAIRWAY_FORCECLEANSTART` needs to be set to false for tests to pass

## Resources
* [Stairway Flight Developer Guide](https://github.com/DataBiosphere/stairway/blob/develop/FLIGHT_DEVELOPER_GUIDE.md) - Data Repo utilizes Stairway to run asynchronous operations throughout the code base.
* [Data Repo Service](https://ga4gh.github.io/data-repository-service-schemas/docs/) - The Data Repo implements parts of the The Data Repository Service (DRS) specification.
