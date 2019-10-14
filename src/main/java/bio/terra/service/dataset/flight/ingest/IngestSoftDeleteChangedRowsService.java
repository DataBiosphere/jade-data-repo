package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Table;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

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
