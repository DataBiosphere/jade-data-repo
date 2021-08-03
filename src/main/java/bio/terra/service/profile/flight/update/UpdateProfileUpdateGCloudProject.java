package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class UpdateProfileUpdateGCloudProject implements Step {
  private final GoogleProjectService googleProjectService;

  public UpdateProfileUpdateGCloudProject(GoogleProjectService projectService) {
    this.googleProjectService = projectService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel newBillingProfileModel =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), BillingProfileModel.class);

    googleProjectService.updateProjectsBillingAccount(newBillingProfileModel);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel existingBillingProfileModel =
        workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);

    googleProjectService.updateProjectsBillingAccount(existingBillingProfileModel);

    return StepResult.getStepResultSuccess();
  }
}
