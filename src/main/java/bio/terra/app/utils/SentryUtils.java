package bio.terra.app.utils;

import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.ConflictException;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.NotImplementedException;
import io.sentry.Sentry;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.servlet.NoHandlerFoundException;

public class SentryUtils {
  public static void initializeSentry(String dsn, String environment) {
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
                if (throwable instanceof NotFoundException
                    || throwable instanceof BadRequestException
                    || throwable instanceof NotImplementedException
                    || throwable instanceof ConflictException
                    || throwable instanceof MethodArgumentNotValidException
                    || throwable instanceof IllegalArgumentException
                    || throwable instanceof NoHandlerFoundException
                    || throwable instanceof ForbiddenException) {
                  return null;
                }
                return event;
              });
        });
  }
}
