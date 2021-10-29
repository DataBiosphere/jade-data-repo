package bio.terra.service.dataset.flight.datadelete;

import bio.terra.model.DataDeletionRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import java.util.UUID;

public final class DataDeletionUtils {

  private DataDeletionUtils() {}

  public static String getSuffix(FlightContext context) {
    return context.getFlightId().replace('-', '_');
  }

  public static DataDeletionRequest getRequest(FlightContext context) {
    return context
        .getInputParameters()
        .get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
  }

  public static Dataset getDataset(FlightContext context, DatasetService datasetService) {
    String datasetId =
        context.getInputParameters().get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
    return datasetService.retrieve(UUID.fromString(datasetId));
  }
}
