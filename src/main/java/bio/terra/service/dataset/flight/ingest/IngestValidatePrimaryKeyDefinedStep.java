package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidIngestPrimaryKeyRequiredException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestValidatePrimaryKeyDefinedStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestValidatePrimaryKeyDefinedStep.class);
  private final DatasetService datasetService;

  public IngestValidatePrimaryKeyDefinedStep(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    DatasetTable targetTable = IngestUtils.getDatasetTable(context, dataset);

    if (targetTable.getPrimaryKey() == null || targetTable.getPrimaryKey().isEmpty()) {
      IngestRequestModel.UpdateStrategyEnum updateStrategy =
          IngestUtils.getIngestRequestModel(context).getUpdateStrategy();
      throw new InvalidIngestPrimaryKeyRequiredException(
          "Target table must have primary key defined.",
          List.of("Update strategy = " + updateStrategy));
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
