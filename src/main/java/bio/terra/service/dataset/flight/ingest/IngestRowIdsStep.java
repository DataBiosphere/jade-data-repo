package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestRowIdsStep implements Step {
  private DatasetService datasetService;
  private BigQueryDatasetPdao bigQueryDatasetPdao;
  private boolean unsetExistingRowIds;

  public IngestRowIdsStep(
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      boolean unsetExistingRowIds) {
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.unsetExistingRowIds = unsetExistingRowIds;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    bigQueryDatasetPdao.addRowIdsToStagingTable(dataset, stagingTableName, unsetExistingRowIds);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // The update will update row ids that are null, so it can be restarted on failure.
    return StepResult.getStepResultSuccess();
  }
}
