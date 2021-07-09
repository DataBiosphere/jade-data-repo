package bio.terra.datarepo.service.dataset.flight.ingest;

import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestRowIdsStep implements Step {
  private DatasetService datasetService;
  private BigQueryPdao bigQueryPdao;

  public IngestRowIdsStep(DatasetService datasetService, BigQueryPdao bigQueryPdao) {
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    String stagingTableName = IngestUtils.getStagingTableName(context);
    bigQueryPdao.addRowIdsToStagingTable(dataset, stagingTableName);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // The update will update row ids that are null, so it can be restarted on failure.
    return StepResult.getStepResultSuccess();
  }
}
