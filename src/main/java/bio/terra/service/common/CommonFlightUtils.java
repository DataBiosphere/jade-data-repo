package bio.terra.service.common;

import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

// Utility methods for interacting with common flight values
public class CommonFlightUtils {

  private static final String JSON_HINT = ".json";

  /** Return when the flight was submitted if the value was specified as an input */
  public static Instant getCreatedAt(FlightContext context) {
    Long createdAtRaw = context.getInputParameters().get(CommonMapKeys.CREATED_AT, Long.class);
    if (createdAtRaw != null) {
      return Instant.ofEpochMilli(createdAtRaw);
    }
    return null;
  }

  /**
   * Obtains flight information of Interest that might be useful in a journal entry. Patterned after
   * an allowList because the flightContext input parameters can contain some sensitive information
   * that we don't want included in a journal entry. The pattern of using the request payload to
   * hold arbitrary objects that can be returned as JSON led to the inclusion of a hint in the key
   * name so that our front end can detect and unwrap this value.
   *
   * @param flightContext
   * @return LinkedHashMap<String, String>
   */
  public static Map<String, String> getFlightInformationOfInterest(
      @NotNull FlightContext flightContext) {
    Map<String, String> results = new LinkedHashMap<>();
    FlightMap flightMap = flightContext.getInputParameters();
    if (flightMap != null) {
      results.put(
          JobMapKeys.DESCRIPTION.getKeyName(),
          flightMap.get(JobMapKeys.DESCRIPTION.getKeyName(), String.class));
      results.put(
          JobMapKeys.REQUEST.getKeyName() + JSON_HINT,
          flightMap.getRaw(JobMapKeys.REQUEST.getKeyName()));
      results.put(
          JobMapKeys.PARENT_FLIGHT_ID.getKeyName(),
          flightMap.getRaw(JobMapKeys.PARENT_FLIGHT_ID.getKeyName()));
      results.put(
          JobMapKeys.STATUS_CODE.getKeyName(),
          flightMap.getRaw(JobMapKeys.STATUS_CODE.getKeyName()));
    }
    results.put("FLIGHT_ID", flightContext.getFlightId());
    results.put("FLIGHT_CLASS", flightContext.getFlightClassName());
    return results;
  }
}
