package bio.terra.common;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import bio.terra.stairway.RetryRuleRandomBackoff;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common methods for building flights */
public final class FlightUtils {
  private static final Logger logger = LoggerFactory.getLogger(FlightUtils.class);

  private FlightUtils() {}

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
}
