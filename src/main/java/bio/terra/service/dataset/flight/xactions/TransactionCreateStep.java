package bio.terra.service.dataset.flight.xactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionCreateStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionCreateStep.class);
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final AuthenticatedUserRequest userReq;
  private final String transactionDescription;
  // If true, saves the transaction as the response to the flight
  private final boolean returnTransaction;

  public TransactionCreateStep(
      DatasetService datasetService,
      BigQueryPdao bigQueryPdao,
      AuthenticatedUserRequest userReq,
      String transactionDescription,
      boolean returnTransaction) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.userReq = userReq;
    this.transactionDescription = transactionDescription;
    this.returnTransaction = returnTransaction;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    TransactionModel transaction =
        bigQueryPdao.insertIntoTransactionTable(
            userReq, dataset, context.getFlightId(), transactionDescription);
    if (returnTransaction) {
      context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), transaction);
    }

    // Placing ID explicitly so be used UnlockTransaction step
    context.getWorkingMap().put(JobMapKeys.TRANSACTION_ID.getKeyName(), transaction.getId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // If a transaction was already inserted, delete it
    UUID transactionId =
        context.getWorkingMap().get(JobMapKeys.TRANSACTION_ID.getKeyName(), UUID.class);
    if (transactionId != null) {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);
      bigQueryPdao.deleteFromTransactionTable(dataset, transactionId);
    }
    return StepResult.getStepResultSuccess();
  }
}
