package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;

public class CreateSnapshotSamGroupStep implements Step {
  private final IamService iamService;

  public CreateSnapshotSamGroupStep(IamService iamService) {
    this.iamService = iamService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    String groupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);
    String groupEmail;

    try {
      groupEmail = iamService.createGroup(groupName);
    } catch (IamConflictException ex) {
      try {
        // if group already exists, and we have access to it, get the email
        // it must've been created in a previous run of this step
        groupEmail = iamService.getGroup(groupName);
      } catch (IamNotFoundException ex2) {
        // if we do not have access to retrieve the group, fail and restart
        // it must've been created by someone else
        return new StepResult(StepStatus.STEP_RESULT_RESTART_FLIGHT);
      }
    }
    // save the group email for later steps
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, groupEmail);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String groupName =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);

    try {
      iamService.deleteGroup(groupName);
    } catch (IamNotFoundException ex) {
      // if group does not exist, nothing to undo
    }
    return StepResult.getStepResultSuccess();
  }
}
