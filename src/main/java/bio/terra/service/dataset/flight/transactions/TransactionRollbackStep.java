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

public class TransactionRollbackStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionRollbackStep.class);
  private final DatasetService datasetService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final UUID transactionId;
  private final AuthenticatedUserRequest userReq;

  public TransactionRollbackStep(
      DatasetService datasetService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      UUID transactionId,
      AuthenticatedUserRequest userReq) {
    this.datasetService = datasetService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.transactionId = transactionId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    TransactionModel transaction =
        bigQueryTransactionPdao.updateTransactionTableStatus(
            userReq, dataset, transactionId, StatusEnum.ROLLED_BACK);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), transaction);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
