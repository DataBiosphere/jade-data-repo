package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.duos.DuosService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CreateSnapshotFirecloudGroupNameStep implements Step {
  private final UUID snapshotId;
  private final IamService iamService;

  public CreateSnapshotFirecloudGroupNameStep(UUID snapshotId, IamService iamService) {
    this.snapshotId = snapshotId;
    this.iamService = iamService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    String groupName = DuosService.constructUniqueFirecloudGroupName(String.valueOf(snapshotId));
    try {
      iamService.getGroup(groupName);
    } catch (IamNotFoundException ex) {
      context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, groupName);
      return StepResult.getStepResultSuccess();
    }

    throw new InterruptedException("Failed to create a unique group name");
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // nothing to undo
    return StepResult.getStepResultSuccess();
  }
}
