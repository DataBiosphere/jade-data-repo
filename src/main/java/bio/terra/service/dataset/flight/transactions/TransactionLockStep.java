package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.TransactionLockException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionLockStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionLockStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final UUID transactionId;
  private final boolean failIfTerminated;
  AuthenticatedUserRequest userRequest;

  public TransactionLockStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      UUID transactionId,
      boolean failIfTerminated,
      AuthenticatedUserRequest userRequest) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.transactionId = transactionId;
    this.failIfTerminated = failIfTerminated;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    TransactionUtils.putTransactionId(context, transactionId);
    TransactionModel transaction =
        bigQueryTransactionPdao.updateTransactionTableLock(
            dataset, transactionId, context.getFlightId(), userRequest);
    if (failIfTerminated && transaction.getStatus() != TransactionModel.StatusEnum.ACTIVE) {
      // This will trigger the undo below so it's ok to have locked the transaction above
      throw new TransactionLockException("Transaction is already terminated", List.of());
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    try {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);
      bigQueryTransactionPdao.updateTransactionTableLock(dataset, transactionId, null, userRequest);
    } catch (Exception e) {
      logger.info("Unlocking transaction", e);
    }
    return StepResult.getStepResultSuccess();
  }
}
