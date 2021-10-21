package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

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

  public static final Predicate<FlightContext> scratchFileCopyNotNeeded =
      flightContext -> {
        var workingMap = flightContext.getWorkingMap();
        Set<String> tableNamesNeedingCopy =
            FlightUtils.getTyped(workingMap, DataDeletionMapKeys.TABLE_NAMES_NEEDING_COPY);
        return tableNamesNeedingCopy.isEmpty();
      };
}
