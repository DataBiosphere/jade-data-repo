package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestInsertIntoDatasetTableStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestInsertIntoDatasetTableStep.class);

  private static final int MAX_FILE_RESULTS = 1000;

  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final AuthenticatedUserRequest userRequest;
  private final boolean autocommit;

  public IngestInsertIntoDatasetTableStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AuthenticatedUserRequest userRequest,
      boolean autocommit) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.userRequest = userRequest;
    this.autocommit = autocommit;
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
      BulkLoadArrayResultModel fileResults =
          workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);
      // Truncate file results if needed to keep the response from exploding in size
      if (CollectionUtils.size(fileResults.getLoadFileResults()) > MAX_FILE_RESULTS) {
        fileResults.loadFileResults(fileResults.getLoadFileResults().subList(0, MAX_FILE_RESULTS));
      }
      ingestResponse.loadResult(fileResults);

      long failedRowCount = workingMap.get(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, Long.class);
      ingestResponse.badRowCount(ingestResponse.getBadRowCount() + failedRowCount);
      ingestResponse.rowCount(ingestResponse.getRowCount() + failedRowCount);
    }

    // If loading from a payload, there is no path to report to the user
    if (IngestUtils.isIngestFromPayload(context.getInputParameters())) {
      ingestResponse.setPath(null);
    }

    workingMap.put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

    UUID transactionId = TransactionUtils.getTransactionId(context);
    bigQueryDatasetPdao.insertIntoDatasetTable(
        dataset, targetTable, stagingTableName, transactionId);
    bigQueryDatasetPdao.insertIntoMetadataTable(
        dataset,
        targetTable.getRowMetadataTableName(),
        stagingTableName,
        userRequest.getEmail(),
        ingestRequest.getLoadTag());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    if (autocommit) {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);
      DatasetTable table = IngestUtils.getDatasetTable(context, dataset);
      UUID transactionId = TransactionUtils.getTransactionId(context);
      try {
        bigQueryTransactionPdao.rollbackDatasetTable(
            dataset, table.getRawTableName(), transactionId);
      } catch (InterruptedException e) {
        logger.warn(
            String.format(
                "Could not rollback data for table %s in transaction %s",
                dataset.toLogString(), transactionId),
            e);
      }
      try {
        bigQueryTransactionPdao.rollbackDatasetMetadataTable(dataset, table, transactionId);
      } catch (InterruptedException e) {
        logger.warn(
            String.format(
                "Could not rollback metadata for table %s in transaction %s",
                dataset.toLogString(), transactionId),
            e);
      }
    }
    // If running in transaction mode, ingested data since we don't want to delete all data related
    // to the transaction and the user should do an explicit rollback
    return StepResult.getStepResultSuccess();
  }
}
