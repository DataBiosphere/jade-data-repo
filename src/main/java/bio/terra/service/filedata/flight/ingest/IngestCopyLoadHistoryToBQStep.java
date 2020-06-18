package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class IngestCopyLoadHistoryToBQStep implements Step {
    private final Logger logger = LoggerFactory.getLogger(IngestBulkFileResponseStep.class);

    private final LoadService loadService;
    private final DatasetService datasetService;
    private final String loadTag;
    private final String datasetIdString;
    private final BigQueryPdao bigQueryPdao;
    private final int fileChunkSize;
    private final int waitSeconds;

    public IngestCopyLoadHistoryToBQStep(LoadService loadService,
                                         DatasetService datasetService,
                                         String loadTag,
                                         String datasetId,
                                         BigQueryPdao bigQueryPdao,
                                         int fileChunkSize,
                                         int waitSeconds) {
        this.loadService = loadService;
        this.loadTag = loadTag;
        this.datasetIdString = datasetId;
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
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

        int chunkNum = 0;
        List<BulkLoadHistoryModel> loadHistoryArray = null;
        String flightId = context.getFlightId();
        String tableName_FlightId = flightId.replaceAll("[^a-zA-Z0-9]", "_");
        try {
            Instant loadTime = context.getStairway().getFlightState(flightId).getSubmitted();
            bigQueryPdao.createStagingLoadHistoryTable(dataset, tableName_FlightId);
            TimeUnit.SECONDS.sleep(waitSeconds);

            while (loadHistoryArray == null || loadHistoryArray.size() == fileChunkSize) {
                loadHistoryArray = loadService.makeLoadHistoryArray(loadId, fileChunkSize, (chunkNum * fileChunkSize));
                chunkNum++;
                // send list plus load_tag, load_time to BQ to be put in a staging table
                if (!loadHistoryArray.isEmpty()) {
                    bigQueryPdao.loadHistoryToStagingTable(
                        dataset,
                        tableName_FlightId,
                        loadTag,
                        loadTime,
                        loadHistoryArray);
                    // Sleep to avoid BQ rate limit error
                    // From quick survey of logs, longest time to complete load query: 3 seconds
                    TimeUnit.SECONDS.sleep(waitSeconds);
                }
            }
            // copy from staging to actual BQ table
            bigQueryPdao.mergeStagingLoadHistoryTable(dataset, tableName_FlightId);
            bigQueryPdao.deleteStagingLoadHistoryTable(dataset, tableName_FlightId);
        } catch (Exception ex) {
            logger.error("Failed during copy of load history to BQ for flight: " + flightId, ex);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        String flightId = context.getFlightId();
        try {
            UUID datasetId = UUID.fromString(datasetIdString);
            Dataset dataset = datasetService.retrieve(datasetId);
            bigQueryPdao.deleteStagingLoadHistoryTable(dataset, flightId);
        } catch (Exception ex) {
            logger.error("Failure deleting load history staging table for flight: " + flightId, ex);
        }
        return StepResult.getStepResultSuccess();
    }

}
