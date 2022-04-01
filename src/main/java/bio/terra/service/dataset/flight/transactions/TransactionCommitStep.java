package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.model.TransactionModel.StatusEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionCommitStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionCommitStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final AuthenticatedUserRequest userReq;
  // If true, saves the transaction as the response to the flight
  private final boolean returnTransaction;
  // Can specify the transaction ID at the flight level or through the flight working map using the
  // "transactionId" key if this value is null
  private final UUID transactionId;

  public TransactionCommitStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      AuthenticatedUserRequest userReq,
      boolean returnTransaction,
      UUID transactionId) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.userReq = userReq;
    this.returnTransaction = returnTransaction;
    this.transactionId = transactionId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    UUID transactionId =
        this.transactionId == null
            ? TransactionUtils.getTransactionId(context)
            : this.transactionId;
    TransactionModel transaction =
        bigQueryTransactionPdao.updateTransactionTableStatus(
            userReq, dataset, transactionId, StatusEnum.COMMITTED);
    if (returnTransaction) {
      context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), transaction);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
