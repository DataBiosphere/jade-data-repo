package bio.terra.service.dataset.flight.transactions.upgrade;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
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
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final UUID datasetId;

  public TransactionUpgradeSingleDatasetStep(
      DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao, UUID datasetId) {
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    bigQueryDatasetPdao.migrateSchemaForTransactions(datasetService.retrieve(datasetId));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
