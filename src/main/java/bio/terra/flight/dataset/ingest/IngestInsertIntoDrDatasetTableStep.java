package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestInsertIntoDrDatasetTableStep implements Step {
    private DrDatasetDao datasetDao;
    private BigQueryPdao bigQueryPdao;

    public IngestInsertIntoDrDatasetTableStep(DrDatasetDao datasetDao, BigQueryPdao bigQueryPdao) {
        this.datasetDao = datasetDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = IngestUtils.getDataset(context, datasetDao);
        Table targetTable = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);

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

        bigQueryPdao.insertIntoDatasetTable(dataset, targetTable, stagingTableName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not need to undo the data insert. BigQuery guarantees that this statement is atomic, so either
        // of the data will be in the table or we will fail and none of the data is in the table. The
        return StepResult.getStepResultSuccess();
    }
}
