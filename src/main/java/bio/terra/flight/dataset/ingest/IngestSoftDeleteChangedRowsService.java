package bio.terra.flight.dataset.ingest;

import bio.terra.dataset.DatasetService;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.tabulardata.google.BigQueryPdao;

public class IngestSoftDeleteChangedRowsService implements Step {
    private DatasetService datasetService;
    private BigQueryPdao bigQueryPdao;

    public IngestSoftDeleteChangedRowsService(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String overlappingTableName = IngestUtils.getOverlappingTableName(context);

        bigQueryPdao.softDeleteChangedOverlappingRows(dataset,
            targetTable,
            overlappingTableName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // TODO: Add undo for softdeleted rows
        return StepResult.getStepResultSuccess();
    }
}
