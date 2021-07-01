package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestInsertIntoDatasetTableStep implements Step {
  private DatasetService datasetService;
  private BigQueryPdao bigQueryPdao;

  public IngestInsertIntoDatasetTableStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    PdaoLoadStatistics loadStatistics = IngestUtils.getIngestStatistics(context);

    IngestResponseModel ingestResponse =
        new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId())
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
    // We do not need to undo the data insert. BigQuery guarantees that this statement is atomic, so
    // either
    // the data will be in the table or we will fail and none of the data is in the table. The
    return StepResult.getStepResultSuccess();
  }
}
