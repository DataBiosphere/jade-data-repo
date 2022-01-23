package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class IngestInsertIntoDatasetTableStep implements Step {
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestInsertIntoDatasetTableStep(
      DatasetService datasetService,
      BigQueryPdao bigQueryPdao,
      AuthenticatedUserRequest userRequest) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);

    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    PdaoLoadStatistics loadStatistics = IngestUtils.getIngestStatistics(context);

    FlightMap workingMap = context.getWorkingMap();

    IngestResponseModel ingestResponse =
        new IngestResponseModel()
            .dataset(dataset.getName())
            .datasetId(dataset.getId())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(loadStatistics.getBadRecords())
            .rowCount(loadStatistics.getRowCount());

    if (IngestUtils.isCombinedFileIngest(context)) {
      BulkLoadArrayResultModel fileLoadResults =
          workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);
      ingestResponse.loadResult(fileLoadResults);
      long failedRowCount = workingMap.get(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, Long.class);
      ingestResponse.badRowCount(ingestResponse.getBadRowCount() + failedRowCount);
      ingestResponse.rowCount(ingestResponse.getRowCount() + failedRowCount);
    }

    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

    UUID transactionId = workingMap.get(JobMapKeys.TRANSACTION_ID.getKeyName(), UUID.class);
    bigQueryPdao.insertIntoDatasetTable(dataset, targetTable, stagingTableName, transactionId);
    bigQueryPdao.insertIntoMetadataTable(
        dataset,
        targetTable.getRowMetadataTableName(),
        stagingTableName,
        userRequest.getEmail(),
        ingestRequest.getLoadTag());

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
