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

import java.util.List;
import java.util.UUID;


public class IngestCopyLoadHistoryToBQStep implements Step {
    private final Logger logger = LoggerFactory.getLogger(IngestBulkFileResponseStep.class);

    private final LoadService loadService;
    private final DatasetService datasetService;
    private final String loadTag;
    private final String datasetIdString;
    private final BigQueryPdao bigQueryPdao;

    public IngestCopyLoadHistoryToBQStep(LoadService loadService,
                                         DatasetService datasetService,
                                         String loadTag,
                                         String datasetId,
                                         BigQueryPdao bigQueryPdao) {
        this.loadService = loadService;
        this.loadTag = loadTag;
        this.datasetIdString = datasetId;
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String loadIdString = workingMap.get(LoadMapKeys.LOAD_ID, String.class);
        UUID loadId = UUID.fromString(loadIdString);
        UUID datasetId = UUID.fromString(datasetIdString);
        Dataset dataset = datasetService.retrieve(datasetId);
        // for load tag, get files in chunk out of postgres table
        // Want this info:
        // load_tag - have this here!
        // load_time - this will just be on the BQ side of things
        // source_name
        // target_path
        // file_id
        // checksum_crc32c
        // checksum_md5

        int chunkSize = 1000;
        int chunkNum = 0;
        List<BulkLoadHistoryModel> loadHistoryArray = null;
        String flightId = context.getFlightId();
        String tableName = "bulk_results_staging_" + flightId;
        try {
            bigQueryPdao.createStagingLoadHistoryTable(dataset, tableName);

            while (loadHistoryArray == null || loadHistoryArray.size() == chunkSize) {
                loadHistoryArray = loadService.makeLoadHistoryArray(loadId, chunkSize, chunkNum);
                chunkNum++;
                // send list plus load_tag to BQ to be put in a temporary table
                // TODO Perform insert of paged info into staging table
            }

            bigQueryPdao.deleteDatasetTable(dataset, tableName);
        } catch (Exception ex) {
            logger.error("Failure deleting load history staging table: " + tableName, ex);
        }


        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        String flightId = context.getFlightId();
        String tableName = "bulk_results_staging_" + flightId;
        try {
            // not sure if I should move this stuff into a helper method?
            UUID datasetId = UUID.fromString(datasetIdString);
            Dataset dataset = datasetService.retrieve(datasetId);
            bigQueryPdao.deleteDatasetTable(dataset, tableName);
        } catch (Exception ex) {
            logger.error("Failure deleting load history staging table: " + tableName, ex);
        }
        return StepResult.getStepResultSuccess();
    }

}
