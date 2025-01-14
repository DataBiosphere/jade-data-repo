package bio.terra.common;

import bio.terra.model.ErrorModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.RetryRuleRandomBackoff;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

/** Common methods for building flights */
public final class FlightUtils {
  private static final Logger logger = LoggerFactory.getLogger(FlightUtils.class);

  private FlightUtils() {}

  /**
   * Build an error model and set it as the response
   *
   * @param context
   * @param message
   * @param responseStatus
   */
  public static void setErrorResponse(
      FlightContext context, String message, HttpStatus responseStatus) {
    ErrorModel errorModel = new ErrorModel().message(message);
    setResponse(context, errorModel, responseStatus);
  }

  /**
   * Set the response and status code in the result map.
   *
   * @param context flight context
   * @param responseObject response object to set
   * @param responseStatus status code to set
   */
  public static void setResponse(
      FlightContext context, Object responseObject, HttpStatus responseStatus) {
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), responseObject);
    workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), responseStatus);
  }

  /**
   * Common logic for deciding if a BigQuery exception is a retry-able IAM propagation error. There
   * is not a specific reason code for the IAM setPolicy failed error. This check is a bit fragile.
   *
   * @param ex exception to test
   * @return true if exception is likely to be an IAM Propagation error.
   */
  public static boolean isBigQueryIamPropagationError(BigQueryException ex) {
    if (StringUtils.equals(ex.getReason(), "invalid")
        && StringUtils.contains(ex.getMessage(), "IAM setPolicy")) {
      logger.info("Caught probable IAM propagation error - retrying", ex);
      return true;
    }
    return false;
  }

  public static RetryRuleRandomBackoff getDefaultRandomBackoffRetryRule(final int maxConcurrency) {
    return new RetryRuleRandomBackoff(500, maxConcurrency, 5);
  }

  public static RetryRuleExponentialBackoff getDefaultExponentialBackoffRetryRule() {
    return new RetryRuleExponentialBackoff(2, 30, 600);
  }

  /**
   * Given a {@link FlightContext} object, look to see if the there is a value in the input map and
   * if not, read it from the working map
   *
   * @param context The FlightContext object to examine
   * @param key The map key to attempt to read values from
   * @param clazz Class used to deserialize the value from the map
   * @param <T> The type of the expected value in the maps
   * @return A typed value from the flight context with type T or null if no value is found
   */
  public static <T> T getContextValue(FlightContext context, String key, Class<T> clazz) {
    T value = context.getInputParameters().get(key, clazz);
    if (value == null) {
      value = context.getWorkingMap().get(key, clazz);
    }
    return value;
  }

  public static <T> T getTyped(FlightMap workingMap, String key) {
    return workingMap.get(key, new TypeReference<>() {});
  }

  public interface Interruptable {
    void run() throws InterruptedException;
  }

  /**
   * Handle the case where an ACL exception is thrown due to the user exhausting the number of
   * authorized entities in the BigQuery dataset. In this case, we want to report the error as a 400
   * Bad Request and provide a message to the user. The original exception is still thrown and will
   * be caught by the Stairway flight runner.
   *
   * @param context the current flight context
   * @param runnable the code to run that may throw a GoogleResourceException
   */
  public static void handleGcpAclException(FlightContext context, Interruptable runnable)
      throws InterruptedException {
    try {
      runnable.run();
    } catch (GoogleResourceException e) {
      if (e.getCause() instanceof BigQueryException bqe
          && bqe.getMessage().startsWith("Too many authorized entities in this dataset.")) {
        setErrorResponse(
            context,
            bqe.getMessage() + " Resolve this by deleting snapshots or creating a second dataset.",
            HttpStatus.BAD_REQUEST);
      }
      throw e;
    }
  }
}
