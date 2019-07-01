package bio.terra.flight.dataset.ingest;

import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCleanupStep implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.dataset.ingest");

    private BigQueryPdao bigQueryPdao;

    public IngestCleanupStep(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We do not want to fail the insert because we fail to cleanup the staging table.
        // We log the failure and move on.
        String stagingTableName = "<unknown>";
        try {
            stagingTableName = IngestUtils.getStagingTableName(context);
            IngestUtils.deleteStagingTable(context, bigQueryPdao);
        } catch (Exception ex) {
            logger.error("Failure deleting ingest staging table: " + stagingTableName, ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
