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
// Appender that sends to a file
appender("File-Appender", FileAppender) {
    file = "${LOG_PATH}/datarepo.log"
    encoder(PatternLayoutEncoder) {
        pattern = "%date %-5level [%thread]: %message%n"
        outputPatternAsHeader = true
    }
}

// You can set different logging configuration. For example, uncommenting the next line
// will set all loggers in the Stairway package to log at debug level:
// logger("bio.terra.stairway", DEBUG)
logger("org.springframework", WARN)
logger("liquibase.executor", WARN)
logger("io.swagger.models.parameters.AbstractSerializableParameter", ERROR)

// root sets the default logging level and appenders
root(INFO, ["Console-Appender", "File-Appender"])
