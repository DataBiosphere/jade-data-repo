package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.LoadService.LoadHistoryIterator;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.*;
import bio.terra.stairway.exception.DatabaseOperationException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Predicate;

public abstract class IngestCopyLoadHistoryStep extends SkippableStep {

  public IngestCopyLoadHistoryStep(Predicate<FlightContext> skipCondition) {
    super(skipCondition);
  }

  public IngestCopyLoadHistoryStep() {
    this(SkippableStep::neverSkip);
  }

  protected IngestCopyLoadHistoryResources getResources(
      FlightContext context,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      int loadHistoryChunkSize)
      throws InterruptedException {
    try {
      FlightMap workingMap = context.getWorkingMap();
      String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);

      UUID loadId = UUID.fromString(loadIdString);
      Dataset dataset = datasetService.retrieve(datasetId);
      Instant loadTime = context.getStairway().getFlightState(context.getFlightId()).getSubmitted();
      LoadHistoryIterator loadHistoryIterator =
          loadService.loadHistoryIterator(loadId, loadHistoryChunkSize);

      return new IngestCopyLoadHistoryResources(dataset, loadTime, loadHistoryIterator);
    } catch (DatabaseOperationException ex) {
      throw new RuntimeException("Failed to get stairway flight state", ex);
    }
  }

  protected static class IngestCopyLoadHistoryResources {
    final Dataset dataset;
    final Instant loadTime;
    final LoadHistoryIterator loadHistoryIterator;

    public IngestCopyLoadHistoryResources(
        Dataset dataset, Instant loadTime, LoadHistoryIterator loadHistoryIterator) {
      this.dataset = dataset;
      this.loadTime = loadTime;
      this.loadHistoryIterator = loadHistoryIterator;
    }
  }
}
