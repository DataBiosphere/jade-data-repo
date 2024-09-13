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

    ./gradlew rollback -PliquibaseCommandValue="study_init"

You can operate on one database or the other by specifying the parameter `runList`. For example, to
just drop tables from stairway, you would run:

    ./gradlew dropAll -PrunList=stairway

### Controlling database connection information

The connection information for the datarepo and stairway databases is stored in the
gradle.properties file. During the build, those properties are substituted into the
application.properties file. You can override the properties in gradle.properties by
creating your own gradle.properties file in the directory referred to by the env var
GRADLE_USER_HOME.
