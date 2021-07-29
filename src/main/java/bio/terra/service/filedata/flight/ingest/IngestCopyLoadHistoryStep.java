package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.LoadService.LoadHistoryIterator;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.*;
import bio.terra.stairway.exception.DatabaseOperationException;
import java.time.Instant;
import java.util.UUID;

public abstract class IngestCopyLoadHistoryStep {

  public IngestCopyLoadHistoryResources getResources(
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

      IngestCopyLoadHistoryResources resources = new IngestCopyLoadHistoryResources();
      resources.loadId = loadId;
      resources.dataset = dataset;
      resources.loadTime = loadTime;
      resources.loadHistoryIterator = loadHistoryIterator;
      return resources;
    } catch (DatabaseOperationException ex) {
      throw new RuntimeException("Failed to get stairway flight state", ex);
    }
  }

  class IngestCopyLoadHistoryResources {
    UUID loadId;
    Dataset dataset;
    Instant loadTime;
    LoadHistoryIterator loadHistoryIterator;
  }
}
