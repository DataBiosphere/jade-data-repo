package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Table;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestSoftDeleteMissingRowsStep implements Step {
    private DatasetService datasetService;
    private BigQueryPdao bigQueryPdao;

    public IngestSoftDeleteMissingRowsStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        Table targetTable  = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);

        // TODO: How to record the number of affected rows, for reporting?
        bigQueryPdao.softDeleteRowsWithNoOverlap(dataset, targetTable, stagingTableName);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We still don't have a way to undo soft-deletes...
        // Could we use partitioning & clustering on the SD tables
        // so DML statements to delete rows don't need to scan the
        // entire table?
        return StepResult.getStepResultSuccess();
    }
}
