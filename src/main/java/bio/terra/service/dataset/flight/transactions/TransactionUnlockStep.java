package bio.terra.service.dataset.flight.transactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionUnlockStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionUnlockStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final UUID transactionId;
  private final AuthenticatedUserRequest userRequest;

  public TransactionUnlockStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      UUID transactionId,
      AuthenticatedUserRequest userRequest) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.transactionId = transactionId;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    UUID transactionId;
    if (this.transactionId != null) {
      transactionId = this.transactionId;
    } else {
      transactionId = TransactionUtils.getTransactionId(context);
    }
    if (transactionId == null) {
      throw new IllegalArgumentException("No transaction ID specified");
    }
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    bigQueryTransactionPdao.updateTransactionTableLock(dataset, transactionId, null, userRequest);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
