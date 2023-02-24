package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.LoadService.LoadHistoryIterator;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.exception.DatabaseOperationException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IngestCopyLoadHistoryStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(IngestCopyLoadHistoryStep.class);

  protected IngestCopyLoadHistoryResources getResources(
      FlightContext context,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      int loadHistoryChunkSize,
      boolean isBulkMode)
      throws InterruptedException {
    try {
      FlightMap workingMap = context.getWorkingMap();

      Dataset dataset = datasetService.retrieve(datasetId);
      Instant loadTime = context.getStairway().getFlightState(context.getFlightId()).getSubmitted();
      LoadHistoryIterator loadHistoryIterator;
      if (isBulkMode) {
        List<BulkLoadHistoryModel> loadResults =
            workingMap.get(IngestMapKeys.BULK_LOAD_HISTORY_RESULT, new TypeReference<>() {});
        // Clear the working map to save space in the DB
        workingMap.putRaw(IngestMapKeys.BULK_LOAD_HISTORY_RESULT, "");
        loadHistoryIterator = loadService.loadHistoryIterator(loadResults, loadHistoryChunkSize);
      } else {
        String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
        UUID loadId = UUID.fromString(loadIdString);
        loadHistoryIterator = loadService.loadHistoryIterator(loadId, loadHistoryChunkSize);
      }

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
