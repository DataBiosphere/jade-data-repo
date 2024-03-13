package bio.terra.service.common.gcs;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.stairway.FlightContext;

public class BigQueryUtils {

  public static String getSuffix(FlightContext context) {
    return getSuffix(context.getFlightId());
  }

  public static String getSuffix(String flightId) {
    return flightId.replace('-', '_');
  }

  public static String gsPathMappingTableName(Snapshot snapshot) {
    return snapshot.getName() + "_gspath_mapping";
  }
}
