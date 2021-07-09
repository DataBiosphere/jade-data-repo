package bio.terra.datarepo.service.dataset.flight.ingest;

import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCleanupStep implements Step {
  private Logger logger =
      LoggerFactory.getLogger("bio.terra.datarepo.service.dataset.flight.ingest");

  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;

  public IngestCleanupStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // We do not want to fail the insert because we fail to cleanup the staging table.
    // We log the failure and move on.
    String stagingTableName = "<unknown>";

    try {
      Dataset dataset = IngestUtils.getDataset(context, datasetService);

      stagingTableName = IngestUtils.getStagingTableName(context);
      bigQueryPdao.deleteDatasetTable(dataset, stagingTableName);
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
