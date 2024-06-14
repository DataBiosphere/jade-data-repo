package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.duos.DuosService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class CreateSnapshotFirecloudGroupStep implements Step {
  private final IamService iamService;
  private final UUID snapshotId;

  public CreateSnapshotFirecloudGroupStep(IamService iamService, UUID snapshotId) {
    this.iamService = iamService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {

    String groupName = DuosService.constructFirecloudGroupName(String.valueOf(snapshotId));
    try {
      iamService.createGroup(groupName);
    } catch (IamConflictException ex) {
      groupName = DuosService.constructUniqueFirecloudGroupName(String.valueOf(snapshotId));
      iamService.createGroup(groupName);
    }

    context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, groupName);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String createdGroup =
        context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, String.class);
    if (createdGroup != null) {
      iamService.deleteGroup(createdGroup);
    }
    return StepResult.getStepResultSuccess();
  }
}
