package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.UndoSuccessStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public record GetSnapshotGoogleProjectIdsStep(SnapshotService snapshotService, UUID datasetId)
    implements UndoSuccessStep {

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    var projectIds = snapshotService.getSnapshotGoogleProjectIds(datasetId);
    flightContext
        .getWorkingMap()
        .put(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, projectIds);
    return StepResult.getStepResultSuccess();
  }
}
