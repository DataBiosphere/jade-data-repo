package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropExternalTablesStep implements Step {

  private final DatasetService datasetService;

  private static Logger logger = LoggerFactory.getLogger(DropExternalTablesStep.class);

  public DropExternalTablesStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = getDataset(context, datasetService);
    String suffix = BigQueryUtils.getSuffix(context);
    DataDeletionRequest dataDeletionRequest = getRequest(context);

    for (DataDeletionTableModel table : dataDeletionRequest.getTables()) {
      BigQueryPdao.deleteExternalTable(dataset, table.getTableName(), suffix);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // TODO: need a human to intervene -- the soft delete worked but we couldn't clean up tables
    return StepResult.getStepResultSuccess();
  }
}
