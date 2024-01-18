package bio.terra.app.configuration;

import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.ConflictException;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.NotImplementedException;
import io.sentry.Sentry;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.servlet.NoHandlerFoundException;

@ConfigurationProperties(prefix = "sentry")
public record SentryConfiguration(String dsn, String environment) {
  private static final Logger logger = LoggerFactory.getLogger(SentryConfiguration.class);
  private static final String DEFAULT_UNDEFINED_ENVIRONMENT = "undefined";
  private static final List<Class<? extends Exception>> FILTERED =
      List.of(
          NotFoundException.class,
          BadRequestException.class,
          NotImplementedException.class,
          ConflictException.class,
          MethodArgumentNotValidException.class,
          IllegalArgumentException.class,
          NoHandlerFoundException.class,
          ForbiddenException.class);

  @PostConstruct
  private void postConstruct() {
    logger.info("Initializing Sentry");
    // in order to filter out exceptions, we must initialize sentry options
    // otherwise, they can be automatically configured via application.properties
    Sentry.init(
        options -> {
          options.setDsn(Objects.requireNonNullElse(this.dsn, ""));
          options.setEnvironment(
              Objects.requireNonNullElse(this.environment, DEFAULT_UNDEFINED_ENVIRONMENT));
          // Filter out exceptions we don't want to send to Sentry
          options.setBeforeSend(
              (event, hint) -> {
                Throwable throwable = event.getThrowable();
                if (FILTERED.stream().anyMatch(e -> e.isInstance(throwable))) {
                  return null;
                }
                return event;
              });
        });
  }
}
