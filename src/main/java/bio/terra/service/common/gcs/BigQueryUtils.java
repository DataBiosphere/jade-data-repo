package bio.terra.service.common.gcs;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.stairway.FlightContext;
import com.google.cloud.bigquery.BigQueryException;

public class BigQueryUtils {

  public static String getSuffix(FlightContext context) {
    return context.getFlightId().replace('-', '_');
  }

  public static String gsPathMappingTableName(Snapshot snapshot) {
    return snapshot.getName() + "_gspath_mapping";
  }

  /**
   * True when BigQuery refused a request for lack of permission, for example creating an external
   * table before a new bucket IAM grant has reached BigQuery.
   */
  public static boolean isAccessDenied(BigQueryException e) {
    return e.getCode() == 403 && "accessDenied".equals(e.getReason());
  }
}
