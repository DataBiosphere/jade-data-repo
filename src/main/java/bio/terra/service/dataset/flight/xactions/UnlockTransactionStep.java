package bio.terra.service.dataset.flight.xactions;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockTransactionStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(UnlockTransactionStep.class);
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final UUID transactionId;

  public UnlockTransactionStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, UUID transactionId) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.transactionId = transactionId;
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
    bigQueryPdao.updateTransactionTableLock(dataset, transactionId, null);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
