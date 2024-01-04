package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCleanupStep implements Step {
  private Logger logger = LoggerFactory.getLogger(IngestCleanupStep.class);

  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  public IngestCleanupStep(DatasetService datasetService, BigQueryDatasetPdao bigQueryDatasetPdao) {
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // We do not want to fail the insert because we fail to cleanup the staging table.
    // We log the failure and move on.
    String stagingTableName = "<unknown>";

    try {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);

      stagingTableName = IngestUtils.getStagingTableName(context);
      bigQueryDatasetPdao.deleteDatasetTable(dataset, stagingTableName);
    } catch (Exception ex) {
      logger.error("Failure deleting ingest staging table: " + stagingTableName, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
