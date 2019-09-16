package bio.terra.flight.dataset.ingest;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
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

        IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
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

        bigQueryPdao.upsertIntoDatasetTable(dataset, targetTable, stagingTableName, overlappingTableName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not need to undo the data insert. BigQuery guarantees that this statement is atomic, so either
        // of the data will be in the table or we will fail and none of the data is in the table. The
        return StepResult.getStepResultSuccess();
    }
}
