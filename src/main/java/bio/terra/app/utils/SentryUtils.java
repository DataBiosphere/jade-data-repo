package bio.terra.app.utils;

import bio.terra.app.configuration.SentryConfiguration;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.ConflictException;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.NotImplementedException;
import io.sentry.Sentry;
import java.util.List;
import java.util.Objects;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.servlet.NoHandlerFoundException;

public class SentryUtils {
  private static final String DEFAULT_UNDEFINED_ENVIRONMENT = "undefined";
  private static final List<Class<? extends Exception>> FILTERED = List.of(
      NotFoundException.class,
      BadRequestException.class,
      NotImplementedException.class,
      ConflictException.class,
      MethodArgumentNotValidException.class,
      IllegalArgumentException.class,
      NoHandlerFoundException.class,
      ForbiddenException.class
  );

  public static void initializeSentry(SentryConfiguration sentryConfiguration) {
    String dsn = Objects.requireNonNullElse(sentryConfiguration.getDsn(), "");
    String environment =
        Objects.requireNonNullElse(
            sentryConfiguration.getEnvironment(), DEFAULT_UNDEFINED_ENVIRONMENT);
    // in order to filter out exceptions, we must initialize sentry options
    // otherwise, they can be automatically configured via application.properties
    Sentry.init(
        options -> {
          options.setDsn(dsn);
          options.setEnvironment(environment);
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
