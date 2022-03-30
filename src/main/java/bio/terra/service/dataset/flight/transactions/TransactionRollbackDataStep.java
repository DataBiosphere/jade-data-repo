package bio.terra.service.dataset.flight.transactions;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionRollbackDataStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionRollbackDataStep.class);
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final UUID transactionId;

  public TransactionRollbackDataStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, UUID transactionId) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
    this.transactionId = transactionId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    for (DatasetTable table : dataset.getTables()) {
      bigQueryPdao.rollbackDatasetTable(dataset, table.getRawTableName(), transactionId);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
