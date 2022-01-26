package bio.terra.service.dataset.flight.xactions.upgrade;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionUpgradeSingleDatasetStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(TransactionUpgradeSingleDatasetStep.class);
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final UUID datasetId;

  public TransactionUpgradeSingleDatasetStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, UUID datasetId) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    bigQueryPdao.migrateSchemaForTransactions(datasetService.retrieve(datasetId));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
