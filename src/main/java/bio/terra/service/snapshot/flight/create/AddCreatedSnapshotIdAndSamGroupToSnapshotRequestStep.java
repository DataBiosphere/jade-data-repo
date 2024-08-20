package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddCreatedSnapshotIdAndSamGroupToSnapshotRequestStep implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;
  private final UUID createdSnapshotId;
  private final String samGroupCreatedByEmail;

  public AddCreatedSnapshotIdAndSamGroupToSnapshotRequestStep(
      SnapshotRequestDao snapshotRequestDao,
      UUID snapshotRequestId,
      UUID snapshotId,
      String samGroupCreatedByEmail) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
    this.createdSnapshotId = snapshotId;
    this.samGroupCreatedByEmail = samGroupCreatedByEmail;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    var workingMap = context.getWorkingMap();
    String samGroupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);
    String samGroupEmail =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class);
    if (samGroupName == null || samGroupEmail == null || samGroupCreatedByEmail == null) {
      throw new IllegalArgumentException(
          "Sam group name, group email, and created by email are required.");
    }

    snapshotRequestDao.updateSamGroup(
        snapshotRequestId, samGroupName, samGroupEmail, samGroupCreatedByEmail);
    snapshotRequestDao.updateCreatedSnapshotId(snapshotRequestId, createdSnapshotId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // remove the written information if the flight fails
    snapshotRequestDao.updateSamGroup(snapshotRequestId, null, null, null);
    snapshotRequestDao.updateCreatedSnapshotId(snapshotRequestId, null);
    return StepResult.getStepResultSuccess();
  }
}
