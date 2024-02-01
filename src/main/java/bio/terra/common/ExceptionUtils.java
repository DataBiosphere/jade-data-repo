package bio.terra.common;

import bio.terra.common.exception.ErrorReportException;
import java.util.List;

public class ExceptionUtils {

  /**
   * Format the output of {@link Exception} objects
   *
   * @param exception the exception to format
   * @return A human-readable version of the object
   */
  public static String formatException(final Exception exception) {
    if (exception instanceof ErrorReportException errorReportException) {
      // Bypass the toString that ErrorReportException has intercepted.
      // Copied from Throwable.toString
      final String className = exception.getClass().getName();
      String message = exception.getLocalizedMessage();
      String mainMessage = (message != null) ? (className + ": " + message) : className;

      List<String> causes = errorReportException.getCauses();
      if (errorReportException.getCause() != null) {
        causes.add(errorReportException.getCause().getMessage());
      }

      return String.format(
          "%s%s",
          mainMessage,
          !causes.isEmpty() ? String.format(" Details: %s", String.join("; ", causes)) : "");
    } else {
      return exception.toString();
    }
  }
}
