package bio.terra.service.dataset.flight.datadelete;

import bio.terra.model.DataDeletionRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;

public final class DataDeletionUtils {

  private DataDeletionUtils() {}

  public static String getSuffix(FlightContext context) {
    return context.getFlightId().replace('-', '_');
  }

  public static DataDeletionRequest getRequest(FlightContext context) {
    return JobMapKeys.REQUEST.get(context.getInputParameters());
  }

  public static Dataset getDataset(FlightContext context, DatasetService datasetService) {
    return datasetService.retrieve(JobMapKeys.DATASET_ID.get(context.getInputParameters()));
  }
}
