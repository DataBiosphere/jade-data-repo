package bio.terra.service.snapshot.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotInitializeProjectStep implements Step {
  private final ResourceService resourceService;
  private final List<Dataset> sourceDatasets;
  private final String snapshotName;

  public CreateSnapshotInitializeProjectStep(
      ResourceService resourceService, List<Dataset> sourceDatasets, String snapshotName) {
    this.resourceService = resourceService;
    this.sourceDatasets = sourceDatasets;
    this.snapshotName = snapshotName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String projectId = workingMap.get(SnapshotWorkingMapKeys.GOOGLE_PROJECT_ID, String.class);

    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
    // Either the project will have been created and we will find it, or we will create.
    UUID projectResourceId;
    try {
      projectResourceId =
          resourceService.initializeSnapshotProject(
              profileModel, projectId, sourceDatasets, snapshotName, snapshotId);
    } catch (GoogleResourceException e) {
      if (e.getCause().getMessage().contains("500 Internal Server Error")) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      } else {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
      }
    }
    workingMap.put(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // At this time we do not delete projects, so no undo
    return StepResult.getStepResultSuccess();
  }
}
