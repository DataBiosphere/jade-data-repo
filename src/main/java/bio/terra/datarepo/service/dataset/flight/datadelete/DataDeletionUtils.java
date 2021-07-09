package bio.terra.datarepo.service.dataset.flight.datadelete;

import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.job.JobMapKeys;
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
