# Getting Started

Ensure that you have access to the required team resources beforehand. If you
encounter a permission error, it is likely because you are missing appropriate
access. Join the `#github` Slack channel, click the lightning bolt in the
channel header, and select `Join DataBiosphere`. Ask for access to Google Groups
including `dsde-engineering`. A colleague on the team will also need to create a
Helm `datarepo` definition for you.

These instructions assume you use MacOS, and that you are on the internal Broad
network or the VPN.

> During this process, you will need your GitHub and Docker Hub username /
password for multiple steps, so make sure to have those handy. If you don't have
those yet, see the section below, otherwise you can skip to [Connect Accounts](#2-connect-accounts)

## 1. Create a GitHub and Docker Hub account

GitHub is where the Broad stores our code and projects. Docker Hub allows the
development team to easily deploy software without having to install lots of
dependencies.

Sign up to these services with your **personal** email:
  * https://github.com/join
  * https://hub.docker.com/signup

## 2. Connect accounts

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

## 3. Install Homebrew

[Homebrew](https://brew.sh/) is a [package manager](https://en.wikipedia.org/wiki/Package_manager)
which enables the installation of software using a single, convenient command
line interface:

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Once Homebrew is installed, there are a number of useful development tools that
should be installed.

1. [Git](https://git-scm.com/) is a version control tool for tracking changes in
projects and code:

```
brew install git
```

2. [jq](https://stedolan.github.io/jq/) is a command line JSON processing tool:

```
brew install jq
```

3. [Docker](https://www.docker.com/) is a tool to deliver software in packages
called containers. Docker for MacOS also includes [Kubernetes](https://kubernetes.io/),
which deploys groups of containers together in clusters. Once installed, you'll
need to run Docker once from your list of Applications:

```
brew cask install docker
open -a Docker
```

4. [Helm](https://helm.sh/) streamlines the process of defining, installing, and
upgrading Kubernetes deployments, which are otherwise challenging to manage:

```
brew install helm
helm repo add stable https://kubernetes-charts.storage.googleapis.com/
helm repo add datarepo-helm https://broadinstitute.github.io/datarepo-helm
helm plugin install https://github.com/thomastaylor312/helm-namespace
helm repo update
```

5. [Skaffold](https://github.com/GoogleContainerTools/skaffold) facilitates the
continuous development of Kubernetes resources. Newer versions are incompatible
with our development environments, so version 1.3.1 is installed instead.

```
brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/5db9ede616f5d681fa9873b150416d6795e0e0e9/Formula/skaffold.rb
```

6. [Vault](https://www.vaultproject.io/) is an encrypted database used to store
many of the team's secrets such as keys and passwords:

```
brew install vault
export VAULT_ADDR=https://clotho.broadinstitute.org:8200
```

7. Much of the code written at the Broad is in [Java](https://en.wikipedia.org/wiki/Java_(programming_language)).
Install the OpenJDK 8 runtime from the [AdoptOpenJDK](https://adoptopenjdk.net/)
project to develop and run Java code:

```
brew tap AdoptOpenJDK/openjdk
brew cask install adoptopenjdk8
```

8. [Google Cloud SDK](https://cloud.google.com/sdk) is a command-line interface
to Google Cloud services. Once it is installed, you'll need to allow auth access
and configure Docker to connect to the appropriate Google Cloud endpoint when
necessary:

```
brew cask install google-cloud-sdk
gcloud auth login
gcloud auth application-default login
gcloud auth configure-docker
```

9. [IntelliJ IDEA](https://www.jetbrains.com/idea/) is an integrated development
environment (IDE) for Java. There are two versions available: **Ultimate** (paid)
and **Community** (open-source). The **Community** edition has all the features
needed for development:

```
brew cask install intellij-idea-ce
open -a "IntelliJ IDEA CE"
```

10. Once it is launched, go to IntelliJ IDEA -> Preferences -> Plugins,
then click in the search box and install **Cloud Code**, which integrates
Google Cloud features with IntelliJ IDEA.

## 4. Create GitHub token

The GitHub token verifies team permissions. This token is necessary for the next
step, [Login to Vault](#6-login-to-vault). To create a token:

1. Go to the [GitHub Personal Access Token](https://github.com/settings/tokens)
page and click **Generate new token**.
2. Give the token a descriptive name, **only** give it the `read:org` scope
under `admin:org`, and click **Generate token**.
3. Store this token in a file:

```
GH_VAULT_TOKEN=<<GITHUB TOKEN VALUE>>
echo $GH_VAULT_TOKEN > ~/.gh_token
```

## 5. Login to Vault

Vault access tokens can be obtained using the GitHub token from earlier as
follows:

```
vault login -method=github token=$(cat ~/.gh_token)
```

## 6. Code Checkout

> It may be useful to create a folder for Broad projects in your home directory.

Download the team's projects:

```
git clone https://github.com/DataBiosphere/jade-data-repo
git clone https://github.com/DataBiosphere/jade-data-repo-ui
git clone https://github.com/DataBiosphere/jade-data-repo-cli
git clone https://github.com/broadinstitute/terraform-jade
git clone https://github.com/broadinstitute/datarepo-helm
git clone https://github.com/broadinstitute/datarepo-helm-definitions
```

## 7. Google Cloud Platform setup

1. Log in to [Google Cloud Platform](https://console.cloud.google.com). In the
top-left corner, select the **BROADINSTITUTE.ORG** organization. Select
**broad-jade-dev** from the list of projects.
2. From the left hand sidebar, select **Kubernetes Engine -> Clusters** under
**COMPUTE**.
3. Click **Connect** on the **dev-master** cluster. This gives you a `kubectl`
command to copy and paste into the terminal:

```
gcloud container clusters get-credentials dev-master --region us-central1 --project broad-jade-dev
```

4. From your [project directory](#7-code-checkout), bring up Helm services:

```
# replace all instances of `zzz` with your initials
cd datarepo-helm-definitions/dev/zzz
helm namespace upgrade zzz-secrets datarepo-helm/create-secret-manager-secret --version=0.0.4 --install --namespace zzz -f zzzSecrets.yaml
helm namespace upgrade zzz-jade datarepo-helm/datarepo --version=0.1.0 --install --namespace zzz -f zzzDeployment.yaml
```

## 8. Install Postgres 12

[Postgres](https://www.postgresql.org/) is an advanced open-source database.
**Postgres.app** is used to manage a local installation of Postgres. The latest
release can be found on the [GitHub releases](https://github.com/PostgresApp/PostgresApp/releases)
page. For compatibility, make sure to select a version which supports all the
older versions of Postgres including 9.6. After launching the application,
create a new version 12 database as follows:

1. Click the sidebar icon (bottom left-hand corner) and then click the plus sign
2. Name the new server, making sure to select version **12**, and then
**Initialize** it
3. Add `/Applications/Postgres.app/Contents/Versions/latest/bin` to your path
(there are multiple ways to achieve this)
4. Create the data repo db and user by running the following while in the `jade-data-repo` directory. More information can be found in [the database readme](https://github.com/DataBiosphere/jade-data-repo/blob/develop/DATABASE.md)
  ```
  psql -f db/create-data-repo-db
  ```
  You can run the following to make sure they've been added:
  This will switch you over to running postgres commands: 
  ```
  psql
  ```
  List out the databases. You should now see "datarepo" and "stairway."
  ```
  \l
  ```

## 9. Setup Repositories
### 1. Build jade-data-repo
Find more detailed instructions on the [main readme](https://github.com/DataBiosphere/jade-data-repo). You can build the repo and then test to make sure it's working as expceted with the following commands.
* You may need to add environmental variables (where 'zzz' is your initials or the environment (i.e. 'dev')):
```
export VAULT_ADDR=https://clotho.broadinstitute.org:8200
export PROXY_URL=https://jade-zzz.datarepo-dev.broadinstitute.org
```
* If you're not on a Broad computer, you made need to also set the host to localhost (as opposed 'http://local.broadinstitute.org')
```
export HOST=localhost
```
* Build jade-data-repo:
```
./gradlew bootRun
```
* Run linters and unit tests:
```
./gradlew check
```
* Run connected and integration tests:
```
./gradlew testConnected
```

### 2. Build jade-data-repo-ui
Follow [setup instructions](https://github.com/DataBiosphere/jade-data-repo-ui#jade-data-repository-ui) for the jade-data-repo-ui. 

### 3. Initialize terraform-jade
Follow instructions found on the [terraform-jade repo readme](https://github.com/broadinstitute/terraform-jade). 

## Common Issues
1. On Broad Non-split VPN. Find instructions [here](https://broad.io/vpn) to set up your VPN. 
2. Docker is running
3. Postgres database is started
4. Environmental variables are set (replace 'zzz' with your initials or "dev" depending on what you're trying to access).
Below you'll find a list of parameters, you may need some or all of these depending on what you're trying to run. 
  * export NVM_DIR="$HOME/.nvm"
  * export VAULT_ADDR=https://clotho.broadinstitute.org:8200
  * export ENVIRONMENT=dev
  * export GOOGLE_CLOUD_PROJECT=broad-jade-zzz
  * export SUFFIX=zzz
  * export STEWARD_ACCT={YOUREMAIL}@{whatever}.com
  * export PROXY_URL=https://jade-zzz.datarepo-dev.broadinstitute.org
  * export CYPRESS_BASE_URL=http://local.broadinstitute.org:3000
  // Only need to do this if you're not using a Broad computer - otherwise host would be http://local.broadinstitute.org
  * export HOST=localhost 