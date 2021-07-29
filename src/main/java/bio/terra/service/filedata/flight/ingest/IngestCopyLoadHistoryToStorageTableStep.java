package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.tabulardata.azure.StorageTableDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
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

public class IngestCopyLoadHistoryToStorageTableStep extends IngestCopyLoadHistoryStep
    implements Step {

  private static final Logger logger =
      LoggerFactory.getLogger(IngestCopyLoadHistoryToStorageTableStep.class);

  private final StorageTableDao storageTableDao;
  private final LoadService loadService;
  private final DatasetService datasetService;
  private final UUID datasetId;
  private final String loadTag;
  private final int loadHistoryChunkSize;

  public IngestCopyLoadHistoryToStorageTableStep(
      StorageTableDao storageTableDao,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      String loadTag,
      int loadHistoryChunkSize) {
    this.storageTableDao = storageTableDao;
    this.loadService = loadService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.loadTag = loadTag;
    this.loadHistoryChunkSize = loadHistoryChunkSize;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var resources =
        getResources(context, loadService, datasetService, datasetId, loadHistoryChunkSize);
    var billingProfile =
        context.getWorkingMap().get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    List<InterruptedException> maybeExceptions =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(resources.loadHistoryIterator, 0), true)
            .map(
                models -> {
                  try {
                    storageTableDao.loadHistoryToAStorageTable(
                        resources.dataset,
                        billingProfile,
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
}
