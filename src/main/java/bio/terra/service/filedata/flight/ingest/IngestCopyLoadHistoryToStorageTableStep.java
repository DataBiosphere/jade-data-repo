package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCopyLoadHistoryToStorageTableStep extends IngestCopyLoadHistoryStep {

  private static final Logger logger =
      LoggerFactory.getLogger(IngestCopyLoadHistoryToStorageTableStep.class);

  private final StorageTableService storageTableService;
  private final LoadService loadService;
  private final DatasetService datasetService;
  private final ProfileService profileService;
  private final UUID datasetId;
  private final UUID profileId;
  private final AuthenticatedUserRequest userReq;
  private final String loadTag;
  private final int loadHistoryChunkSize;

  public IngestCopyLoadHistoryToStorageTableStep(
      StorageTableService storageTableService,
      LoadService loadService,
      DatasetService datasetService,
      ProfileService profileService,
      UUID datasetId,
      UUID profileId,
      AuthenticatedUserRequest userReq,
      String loadTag,
      int loadHistoryChunkSize) {
    this.storageTableService = storageTableService;
    this.loadService = loadService;
    this.datasetService = datasetService;
    this.profileService = profileService;
    this.datasetId = datasetId;
    this.profileId = profileId;
    this.userReq = userReq;
    this.loadTag = loadTag;
    this.loadHistoryChunkSize = loadHistoryChunkSize;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var resources =
        getResources(context, loadService, datasetService, datasetId, loadHistoryChunkSize);

    List<InterruptedException> maybeExceptions =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(resources.loadHistoryIterator, 0), true)
            .map(
                models -> {
                  try {
                    storageTableService.loadHistoryToAStorageTable(
                        resources.dataset,
                        context.getFlightId(),
                        loadTag,
                        resources.loadTime,
                        models);
                    return null;
                  } catch (InterruptedException ex) {
                    return ex;
                  }
                })
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());
    if (!maybeExceptions.isEmpty()) {
      for (var iex : maybeExceptions) {
        logger.error("Encountered error while loading history into storage table", iex);
      }
      var ex =
          new AzureResourceException(
              "Could not load entities into load history storage table", maybeExceptions.get(0));
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    logger.info("No load history staging table for Azure");
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult doSkippableStep(FlightContext flightContext) throws InterruptedException {
    return null;
  }
}
