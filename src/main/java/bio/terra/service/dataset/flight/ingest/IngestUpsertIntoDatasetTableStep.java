package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestUpsertIntoDatasetTableStep implements Step {
    private DatasetService datasetService;
    private BigQueryPdao bigQueryPdao;

    public IngestUpsertIntoDatasetTableStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        String overlappingTableName = IngestUtils.getOverlappingTableName(context);

        bigQueryPdao.upsertIntoDatasetTable(dataset, targetTable, stagingTableName, overlappingTableName);

        IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
        // FIXME: Using the initial load stats is misleading in the upsert case.
        //  Ideally we'd report separate numbers for:
        //    1. Number of new rows added
        //    2. Number of rows updated
        //    3. Number of rows unchanged
        //  The append case could use the same model, with the existing counts all going into bin 1.
        PdaoLoadStatistics loadStatistics = IngestUtils.getIngestStatistics(context);

        IngestResponseModel ingestResponse = new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId().toString())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(loadStatistics.getBadRecords())
            .rowCount(loadStatistics.getRowCount());
        context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not need to undo the data insert. BigQuery guarantees that this statement is atomic, so either
        // of the data will be in the table or we will fail and none of the data is in the table. The
        return StepResult.getStepResultSuccess();
    }
}
