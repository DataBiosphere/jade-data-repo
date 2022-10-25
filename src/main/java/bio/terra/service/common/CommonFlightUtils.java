package bio.terra.service.common;

import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

// Utility methods for interacting with common flight values
public class CommonFlightUtils {

  private static final Set<String> ENTRIES_TO_REMOVE =
      Collections.singleton(JobMapKeys.AUTH_USER_INFO.getKeyName());

  /** Return when the flight was submitted if the value was specified as an input */
  public static Instant getCreatedAt(FlightContext context) {
    Long createdAtRaw = context.getInputParameters().get(CommonMapKeys.CREATED_AT, Long.class);
    if (createdAtRaw != null) {
      return Instant.ofEpochMilli(createdAtRaw);
    }
    return null;
  }

  public static Map getFlightInformationOfInterest(
      FlightMap flightMap, FlightContext flightContext) {
    Map<String, String> results = new LinkedHashMap<>();
    results.put(
        JobMapKeys.DESCRIPTION.getKeyName(), flightMap.getRaw(JobMapKeys.DESCRIPTION.getKeyName()));
    results.put(JobMapKeys.REQUEST.getKeyName(), flightMap.getRaw(JobMapKeys.REQUEST.getKeyName()));
    results.put(
        JobMapKeys.PARENT_FLIGHT_ID.getKeyName(),
        flightMap.getRaw(JobMapKeys.PARENT_FLIGHT_ID.getKeyName()));
    results.put(
        JobMapKeys.STATUS_CODE.getKeyName(), flightMap.getRaw(JobMapKeys.STATUS_CODE.getKeyName()));
    results.put("FLIGHT_ID", flightContext.getFlightId());
    results.put("FLIGHT_CLASS", flightContext.getFlightClassName());
    return results;
  }
}
