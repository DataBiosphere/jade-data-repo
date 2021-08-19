package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.load.LoadService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCopyLoadHistoryToBQStep extends IngestCopyLoadHistoryStep {

  private static final Logger logger = LoggerFactory.getLogger(IngestCopyLoadHistoryToBQStep.class);

  private final BigQueryPdao bigQueryPdao;
  private final LoadService loadService;
  private final DatasetService datasetService;

  private final UUID datasetId;
  private final String loadTag;
  private final int waitSeconds;
  private final int loadHistoryChunkSize;

  public IngestCopyLoadHistoryToBQStep(
      BigQueryPdao bigQueryPdao,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      String loadTag,
      int waitSeconds,
      int loadHistoryChunkSize,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.bigQueryPdao = bigQueryPdao;
    this.loadService = loadService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.loadTag = loadTag;
    this.waitSeconds = waitSeconds;
    this.loadHistoryChunkSize = loadHistoryChunkSize;
  }

  public IngestCopyLoadHistoryToBQStep(
      BigQueryPdao bigQueryPdao,
      LoadService loadService,
      DatasetService datasetService,
      UUID datasetId,
      String loadTag,
      int waitSeconds,
      int loadHistoryChunkSize) {
    this.bigQueryPdao = bigQueryPdao;
    this.loadService = loadService;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
    this.loadTag = loadTag;
    this.waitSeconds = waitSeconds;
    this.loadHistoryChunkSize = loadHistoryChunkSize;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) throws InterruptedException {
    IngestCopyLoadHistoryResources resources =
        getResources(context, loadService, datasetService, datasetId, loadHistoryChunkSize);
    String tableNameFlightId = context.getFlightId().replaceAll("[^a-zA-Z0-9]", "_");
    try {
      bigQueryPdao.createStagingLoadHistoryTable(resources.dataset, tableNameFlightId);
      TimeUnit.SECONDS.sleep(waitSeconds);
      while (resources.loadHistoryIterator.hasNext()) {
        var array = resources.loadHistoryIterator.next();
        // send list plus load_tag, load_time to BQ to be put in a staging table
        bigQueryPdao.loadHistoryToStagingTable(
            resources.dataset, tableNameFlightId, loadTag, resources.loadTime, array);

        // Sleep to avoid BQ rate limit error
        // From quick survey of logs, longest time to complete load query: 3 seconds
        TimeUnit.SECONDS.sleep(waitSeconds);
      }

      // copy from staging to actual BQ table
      bigQueryPdao.mergeStagingLoadHistoryTable(resources.dataset, tableNameFlightId);
      bigQueryPdao.deleteStagingLoadHistoryTable(resources.dataset, tableNameFlightId);

      return StepResult.getStepResultSuccess();
    } catch (InterruptedException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
  }

  @Override
  public StepResult undoSkippableStep(FlightContext context) {
    String flightId = context.getFlightId();
    try {
      Dataset dataset = datasetService.retrieve(datasetId);
      bigQueryPdao.deleteStagingLoadHistoryTable(dataset, flightId);
    } catch (Exception ex) {
      logger.error("Failure deleting load history staging table for flight: " + flightId, ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
