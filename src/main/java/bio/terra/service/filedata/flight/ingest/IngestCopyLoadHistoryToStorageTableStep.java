package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.load.LoadService;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Predicate;
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
  private final UUID datasetId;
  private final String loadTag;
  private final int loadHistoryChunkSize;

  public IngestCopyLoadHistoryToStorageTableStep(
      StorageTableService storageTableService,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      String loadTag,
      int loadHistoryChunkSize,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.storageTableService = storageTableService;
    this.loadService = loadService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.loadTag = loadTag;
    this.loadHistoryChunkSize = loadHistoryChunkSize;
  }

  public IngestCopyLoadHistoryToStorageTableStep(
      StorageTableService storageTableService,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      String loadTag,
      int loadHistoryChunkSize) {
    this(
        storageTableService,
        loadService,
        datasetService,
        datasetId,
        loadTag,
        loadHistoryChunkSize,
        SkippableStep::neverSkip);
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
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
  public StepResult undoSkippableStep(FlightContext context) {
    logger.info("No load history staging table for Azure");
    return StepResult.getStepResultSuccess();
  }
}
