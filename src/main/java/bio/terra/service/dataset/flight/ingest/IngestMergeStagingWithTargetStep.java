package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.transactions.TransactionUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestMergeStagingWithTargetStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestMergeStagingWithTargetStep.class);
  private final DatasetService datasetService;

  public IngestMergeStagingWithTargetStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    UUID transactionId = TransactionUtils.getTransactionId(context);

    BigQueryDatasetPdao.mergeIngest(dataset, targetTable, stagingTableName, transactionId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Because this step modifies data in a staging / temporary table,
    // undoing it can be a no-op given that the staging table will be deleted
    // later in the process of walking back a flight.
    return StepResult.getStepResultSuccess();
  }
}
