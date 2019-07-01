### Install

Install [postgres server version 9.6](https://github.com/PostgresApp/PostgresApp/releases/download/v2.2/Postgres-2.2-9.5-9.6-10-11.dmg)

This version defaults to starting postgres server version 11. To change to version 9.6:

1. Open postgres app
2. Stop the server
2. Click the icon on the lower left to show the servers
3. Select Postgres11 and then -
4. Select + and choose version 9.6
5. Start the server

## Setting up the database
You'll want to follow all the steps below and stop once you have updated to the latest migration.

### Create the data repo db and user

    psql -f db/create-data-repo-db

### Clear the database

    psql -f db/truncate-tables datarepo drmanager

### Migrations

Database updates are done automatically when the service starts, so you don't have to do anything
special to perform the migration. Simply running the new version is sufficient.

There are many Liquibase commands available via the Gradle plug-in. These are not run as
part of the build, but are handy during development:

To remove all database tables and clear liquibase history, run:

    ./gradlew dropAll

To perform an upgrade to your current schema, run:

    ./gradlew update

In the case where you'd like to go back to a specific migration marked by a tag, run:

    ./gradlew rollback -PliquibaseCommandValue="dataset_init"

You can operate on one database or the other by specifying the parameter `runList`. For example, to
just drop tables from stairway, you would run:

    ./gradlew dropAll -PrunList=stairway

### Controlling database connection information

The connection information for the datarepo and stairway databases is stored in the
gradle.properties file. During the build, those properties are substituted into the
application.properties file. You can override the properties in gradle.properties by
creating your own gradle.properties file in the directory referred to by the env var
GRADLE_USER_HOME.
