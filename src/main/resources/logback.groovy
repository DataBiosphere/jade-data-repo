// LogBack Configuration File
// This file controls how, where, and what gets logged.
// For more information see: https://logback.qos.ch/manual/groovy.html

// Frequency of checking this file for changes and altering the logging.
scan("30 seconds")
// Where the logs will go
def LOG_PATH = "logs"

// Appender that sends to the console
appender("Console-Appender", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date %-5level [%thread] %logger{36}: %message%n"
    }
}

// You can set different logging configuration. For example, uncommenting the next line
// will set all loggers in the Stairway package to log at debug level:
logger("org.springframework", WARN)
logger("liquibase.executor", WARN)
logger("io.swagger.models.parameters.AbstractSerializableParameter", ERROR)

// enable the next 4 lines to see acl and policies
logger("bio.terra.service.dataset.flight.create.CreateDatasetAuthzIamStep", DEBUG);
logger("bio.terra.service.dataset.flight.create.CreateDatasetAuthzPrimaryDataStep", DEBUG);
logger("bio.terra.service.iam.sam.SamIam", DEBUG);
logger("bio.terra.service.tabulardata.google.BigQueryProject", DEBUG);


// root sets the default logging level and appenders
root(INFO, ["Console-Appender"])
