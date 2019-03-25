package bio.terra.flight.study.ingest;

import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestCleanupStep implements Step {
    private BigQueryPdao bigQueryPdao;

    public IngestCleanupStep(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        IngestUtils.deleteStagingTable(context, bigQueryPdao);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be rerun on failure.
        return StepResult.getStepResultSuccess();
    }
}
