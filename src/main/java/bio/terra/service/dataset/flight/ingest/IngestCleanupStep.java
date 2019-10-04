package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCleanupStep implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.service.dataset.ingest");

    private final DatasetService datasetService;
    private final BigQueryPdao bigQueryPdao;

    public IngestCleanupStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We do not want to fail the insert because we fail to cleanup the staging table.
        // We log the failure and move on.
        String stagingTableName = "<unknown>";
        String overlappingTableName = "<unknown>";

        try {
            Dataset dataset = IngestUtils.getDataset(context, datasetService);

            stagingTableName = IngestUtils.getStagingTableName(context);
            bigQueryPdao.deleteDatasetTable(dataset, stagingTableName);
        } catch (Exception ex) {
            logger.error("Failure deleting ingest staging table: " + stagingTableName, ex);
        }

        try {
            Dataset dataset = IngestUtils.getDataset(context, datasetService);

            overlappingTableName = IngestUtils.getOverlappingTableName(context);
            if (bigQueryPdao.tableExists(dataset, overlappingTableName)) {
                bigQueryPdao.deleteDatasetTable(dataset, overlappingTableName);
            }
        } catch (Exception ex) {
            logger.error("Failure deleting overlapping table: " + overlappingTableName, ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
