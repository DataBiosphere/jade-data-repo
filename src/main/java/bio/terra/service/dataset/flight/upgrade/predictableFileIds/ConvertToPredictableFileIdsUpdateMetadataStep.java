package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class ConvertToPredictableFileIdsUpdateMetadataStep implements Step {

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

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Boolean usesPredictableIdsAtStart =
        context
            .getWorkingMap()
            .get(ConvertFileIdUtils.DATASET_USES_PREDICTABLE_IDS_AT_START, Boolean.class);
    if (!usesPredictableIdsAtStart) {
      datasetService.setPredictableFileIds(datasetId, false);
    }
    return StepResult.getStepResultSuccess();
  }
}
