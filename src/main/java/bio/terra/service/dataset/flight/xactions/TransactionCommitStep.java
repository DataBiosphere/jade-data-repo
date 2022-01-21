package bio.terra.service.dataset.flight.xactions;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.TransactionModel;
import bio.terra.model.TransactionModel.StatusEnum;
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

public class TransactionCommitStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionCommitStep.class);
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final AuthenticatedUserRequest userReq;

  public TransactionCommitStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, AuthenticatedUserRequest userReq) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    UUID transactionId = TransactionUtils.getTransactionId(context);
    TransactionModel transaction =
        bigQueryPdao.updateTransactionTableStatus(
            userReq, dataset, transactionId, StatusEnum.COMMITTED);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), transaction);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
