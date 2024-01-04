package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.exception.TooManyDmlStatementsOutstandingException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class TransactionOpenStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionOpenStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final AuthenticatedUserRequest userReq;
  private final String transactionDescription;
  // If true, saves the transaction as the response to the flight
  private final boolean returnTransaction;
  // If true, makes sure to delete the transaction in the undo phase of the flight.
  // Note: this should be false for flights that use this step to create auto-commit transactions
  // (e.g. ingest)
  private final boolean deleteOnUndo;

  public TransactionOpenStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      AuthenticatedUserRequest userReq,
      String transactionDescription,
      boolean returnTransaction,
      boolean deleteOnUndo) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.userReq = userReq;
    this.transactionDescription = transactionDescription;
    this.returnTransaction = returnTransaction;
    this.deleteOnUndo = deleteOnUndo;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    try {
      TransactionModel transaction =
          bigQueryTransactionPdao.insertIntoTransactionTable(
              userReq, dataset, context.getFlightId(), transactionDescription);
      FlightMap workingMap = context.getWorkingMap();
      if (returnTransaction) {
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), transaction);
      }
      // Placing ID explicitly so be used UnlockTransaction step
      TransactionUtils.putTransactionId(context, transaction.getId());
    } catch (TooManyDmlStatementsOutstandingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    } catch (InterruptedException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    if (deleteOnUndo) {
      // If a transaction was already inserted, delete it
      UUID transactionId = TransactionUtils.getTransactionId(context);
      if (transactionId != null) {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        bigQueryTransactionPdao.deleteFromTransactionTable(dataset, transactionId);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
