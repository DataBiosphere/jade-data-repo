package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateProfileUpdateGCloudProject implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(UpdateProfileUpdateGCloudProject.class);
  private final GoogleProjectService googleProjectService;

  public UpdateProfileUpdateGCloudProject(GoogleProjectService projectService) {
    this.googleProjectService = projectService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel newBillingProfileModel = JobMapKeys.RESPONSE.get(workingMap);

    googleProjectService.updateProjectsBillingAccount(newBillingProfileModel);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel existingBillingProfileModel = JobMapKeys.REVERT_TO.get(workingMap);

    googleProjectService.updateProjectsBillingAccount(existingBillingProfileModel);

    return StepResult.getStepResultSuccess();
  }
}
