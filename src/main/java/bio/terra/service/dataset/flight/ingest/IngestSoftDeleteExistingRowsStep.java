package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestSoftDeleteExistingRowsStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestSoftDeleteExistingRowsStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final AuthenticatedUserRequest userReq;
  private final boolean autocommit;

  public IngestSoftDeleteExistingRowsStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AuthenticatedUserRequest userReq,
      boolean autocommit) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.userReq = userReq;
    this.autocommit = autocommit;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    UUID transactionId = TransactionUtils.getTransactionId(context);

    if (targetTable.getPrimaryKey() != null && !targetTable.getPrimaryKey().isEmpty()) {
      logger.info("Removing target rows being replaced from table {}", targetTable.toLogString());
      bigQueryDatasetPdao.insertIntoSoftDeleteDatasetTable(
          userReq,
          dataset,
          targetTable,
          stagingTableName,
          ingestRequest.getLoadTag(),
          context.getFlightId(),
          transactionId);
    } else {
      logger.info("No primary key defined for table {}. Skipping", targetTable.toLogString());
    }

    // TODO<DR-2407>: add something to ingest statistics
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    if (autocommit) {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);
      DatasetTable table = IngestUtils.getDatasetTable(context, dataset);
      UUID transactionId = TransactionUtils.getTransactionId(context);
      try {
        bigQueryTransactionPdao.rollbackDatasetTable(
            dataset, table.getSoftDeleteTableName(), transactionId);
      } catch (InterruptedException e) {
        logger.warn(
            String.format(
                "Could not rollback soft delete data for table %s in transaction %s",
                dataset.toLogString(), transactionId),
            e);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
