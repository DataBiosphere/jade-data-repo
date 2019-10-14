package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Table;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestEvaluateOverlapStep implements Step {
    private DatasetService datasetService;
    private BigQueryPdao bigQueryPdao;

    public IngestEvaluateOverlapStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        String overlappingTableName = IngestUtils.getOverlappingTableName(context);

        bigQueryPdao.loadOverlapTable(dataset, targetTable, stagingTableName, overlappingTableName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not need to undo the data insert into the soft deletes table. BigQuery guarantees that this statement
        // is atomic, so either the data will be in the table or we will fail and none of the data is in the table.
        return StepResult.getStepResultSuccess();
    }
}
