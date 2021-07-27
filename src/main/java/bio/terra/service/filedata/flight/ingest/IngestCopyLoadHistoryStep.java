package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.NotImplementedException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.LoadService.LoadHistoryIterator;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.*;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCopyLoadHistoryStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(IngestBulkFileResponseStep.class);

  private final LoadService loadService;
  private final DatasetService datasetService;
  private final String loadTag;
  private final String datasetIdString;
  private final BigQueryPdao bigQueryPdao;
  private final CloudPlatformWrapper platform;
  private final int fileChunkSize;
  private final int waitSeconds;

  public IngestCopyLoadHistoryStep(
      LoadService loadService,
      DatasetService datasetService,
      String loadTag,
      String datasetId,
      BigQueryPdao bigQueryPdao,
      CloudPlatformWrapper platform,
      int fileChunkSize,
      int waitSeconds) {
    this.loadService = loadService;
    this.loadTag = loadTag;
    this.datasetIdString = datasetId;
    this.bigQueryPdao = bigQueryPdao;
    this.datasetService = datasetService;
    this.platform = platform;
    this.fileChunkSize = fileChunkSize;
    this.waitSeconds = waitSeconds;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
    UUID loadId = UUID.fromString(loadIdString);
    UUID datasetId = UUID.fromString(datasetIdString);
    Dataset dataset = datasetService.retrieve(datasetId);

    String flightId = context.getFlightId();
    String tableName_FlightId = flightId.replaceAll("[^a-zA-Z0-9]", "_");
    try {
      Instant loadTime = context.getStairway().getFlightState(flightId).getSubmitted();
      LoadHistoryIterator historyIterator = loadService.loadHistoryIterator(loadId, fileChunkSize);

      if (platform.isGcp()) {
        doGcpStep(dataset, tableName_FlightId, loadTime, historyIterator);
      } else {
        doAzureStep();
      }
    } catch (Exception ex) {
      logger.error("Failed during copy of load history to BQ for flight: " + flightId, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    String flightId = context.getFlightId();
    if (platform.isGcp()) {
      undoGcpStep(flightId);
    } else {
      undoAzureStep();
    }
    return StepResult.getStepResultSuccess();
  }

  private void doGcpStep(
      Dataset dataset,
      String tableName,
      Instant loadTime,
      LoadService.LoadHistoryIterator loadHistoryIterator)
      throws Exception {
    bigQueryPdao.createStagingLoadHistoryTable(dataset, tableName);
    TimeUnit.SECONDS.sleep(waitSeconds);
    while (loadHistoryIterator.hasNext()) {
      var array = loadHistoryIterator.next();
      // send list plus load_tag, load_time to BQ to be put in a staging table
      bigQueryPdao.loadHistoryToStagingTable(dataset, tableName, loadTag, loadTime, array);

      // Sleep to avoid BQ rate limit error
      // From quick survey of logs, longest time to complete load query: 3 seconds
      TimeUnit.SECONDS.sleep(waitSeconds);
    }

    // copy from staging to actual BQ table
    bigQueryPdao.mergeStagingLoadHistoryTable(dataset, tableName);
    bigQueryPdao.deleteStagingLoadHistoryTable(dataset, tableName);
  }

  private void doAzureStep() {
    throw new NotImplementedException("Azure load history not yet implemented");
  }

  private void undoGcpStep(String flightId) {
    try {
      UUID datasetId = UUID.fromString(datasetIdString);
      Dataset dataset = datasetService.retrieve(datasetId);
      bigQueryPdao.deleteStagingLoadHistoryTable(dataset, flightId);
    } catch (Exception ex) {
      logger.error("Failure deleting load history staging table for flight: " + flightId, ex);
    }
  }

  private void undoAzureStep() {
    logger.error("Should never be an azure step");
  }
}
