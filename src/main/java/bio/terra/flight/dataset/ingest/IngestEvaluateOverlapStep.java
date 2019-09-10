package bio.terra.flight.dataset.ingest;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;

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

        List<String> overlappingRows = bigQueryPdao.getOverLappingRows(dataset, targetTable, stagingTableName);
        List<String> changedOverlappingRows = bigQueryPdao.getChangedOverlappingRows(dataset,
            targetTable,
            overlappingRows);
        bigQueryPdao.softDeleteRows(dataset, targetTable.getName(), dataset.getDataProjectId(), changedOverlappingRows);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not need to undo the data insert into the soft deletes table. BigQuery guarantees that this statement
        // is atomic, so eithe the data will be in the table or we will fail and none of the data is in the table.
        return StepResult.getStepResultSuccess();
    }
}
