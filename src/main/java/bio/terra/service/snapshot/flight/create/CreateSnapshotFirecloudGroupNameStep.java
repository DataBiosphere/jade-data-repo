package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CreateSnapshotFirecloudGroupNameStep extends DefaultUndoStep {
  private final UUID snapshotId;
  private final IamService iamService;

  public CreateSnapshotFirecloudGroupNameStep(UUID snapshotId, IamService iamService) {
    this.snapshotId = snapshotId;
    this.iamService = iamService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    String groupName = IamService.constructFirecloudGroupName(String.valueOf(snapshotId));
    try {
      iamService.getGroup(groupName);
    } catch (IamNotFoundException ex) {
      context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, groupName);
      return StepResult.getStepResultSuccess();
    }

    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new InternalServerErrorException("Group " + groupName + " already exists in Sam"));
  }
}
