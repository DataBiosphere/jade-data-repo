package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class ConvertToPredictableFileIdsUpdateMetadataStep extends DefaultUndoStep {

  private final UUID datasetId;
  private final DatasetService datasetService;

  public ConvertToPredictableFileIdsUpdateMetadataStep(
      UUID datasetId, DatasetService datasetService) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    datasetService.setPredictableFileIds(datasetId, true);
    return StepResult.getStepResultSuccess();
  }
}
