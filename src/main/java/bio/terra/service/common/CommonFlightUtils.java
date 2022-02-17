package bio.terra.service.common;

import bio.terra.stairway.FlightContext;
import java.time.Instant;

// Utility methods for interacting with common flight values
public class CommonFlightUtils {

  /** Return when the flight was submitted if the value was specified as an input */
  public static Instant getCreatedAt(FlightContext context) {
    Long createdAtRaw = context.getInputParameters().get(CommonMapKeys.CREATED_AT, Long.class);
    if (createdAtRaw != null) {
      return Instant.ofEpochMilli(createdAtRaw);
    }
    return null;
  }
}
