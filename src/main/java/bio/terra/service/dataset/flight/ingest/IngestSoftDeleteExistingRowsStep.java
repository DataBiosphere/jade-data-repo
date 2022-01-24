package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.xactions.TransactionUtils;
import bio.terra.service.tabulardata.google.BigQueryPdao;
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
  private final BigQueryPdao bigQueryPdao;
  private final AuthenticatedUserRequest userReq;

  public IngestSoftDeleteExistingRowsStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, AuthenticatedUserRequest userReq) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    UUID transactionId = TransactionUtils.getTransactionId(context);

    if (targetTable.getPrimaryKey() != null && !targetTable.getPrimaryKey().isEmpty()) {
      logger.info(
          "Removing target rows being replaced from table {}", targetTable.toPrintableString());
      bigQueryPdao.insertIntoSoftDeleteDatasetTable(
          userReq,
          dataset,
          targetTable,
          stagingTableName,
          ingestRequest.getLoadTag(),
          context.getFlightId(),
          transactionId);
    } else {
      logger.info("No primary key defined for table {}. Skipping", targetTable.toPrintableString());
    }

    // TODO: should this add something to ingest statistics?
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // TODO: we can delete by flight id
    return StepResult.getStepResultSuccess();
  }
}
