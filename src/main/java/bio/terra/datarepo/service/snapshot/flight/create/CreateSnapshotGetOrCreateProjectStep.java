package bio.terra.datarepo.service.snapshot.flight.create;

import bio.terra.datarepo.app.model.GoogleRegion;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.profile.flight.ProfileMapKeys;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.datarepo.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.datarepo.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class CreateSnapshotGetOrCreateProjectStep implements Step {
  private final ResourceService resourceService;
  private final GoogleRegion region;

  public CreateSnapshotGetOrCreateProjectStep(
      ResourceService resourceService, GoogleRegion region) {
    this.resourceService = resourceService;
    this.region = region;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
    // Either the project will have been created8and we will find it, or we will create.
    UUID projectResourceId;
    try {
      projectResourceId = resourceService.getOrCreateSnapshotProject(profileModel, region);
    } catch (GoogleResourceNamingException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
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
