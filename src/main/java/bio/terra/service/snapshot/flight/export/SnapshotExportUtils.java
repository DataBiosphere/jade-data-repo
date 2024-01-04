package bio.terra.service.snapshot.flight.export;

import bio.terra.stairway.FlightContext;

public class SnapshotExportUtils {

  public static String getFileName(FlightContext context) {
    return String.format("%s_export_gs_path_mapping/gs_path_mapping.json", context.getFlightId());
  }
}
