package bio.terra.flight.dataset.ingest;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.Set;

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

        Set<String> overlappingRows = bigQueryPdao.getOverLappingRows(dataset, targetTable, stagingTableName);
        Set<String> changedOverlappingRows = bigQueryPdao.getChangedOverlappingRows(dataset,
            targetTable,
            overlappingRows);
        bigQueryPdao.softDeleteRows(dataset, targetTable.getName(), dataset.getDataProjectId(), changedOverlappingRows);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
