package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddCreatedInfoToSnapshotRequestStep implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;
  private final UUID createdSnapshotId;
  private final String samGroupCreatedBy;

  public AddCreatedInfoToSnapshotRequestStep(
      SnapshotRequestDao snapshotRequestDao,
      UUID snapshotRequestId,
      UUID snapshotId,
      String samGroupCreatedBy) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
    this.createdSnapshotId = snapshotId;
    this.samGroupCreatedBy = samGroupCreatedBy;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    var workingMap = context.getWorkingMap();
    String samGroupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);

    snapshotRequestDao.updateCreatedInfo(
        snapshotRequestId, createdSnapshotId, samGroupName, samGroupCreatedBy);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // remove the written information if the flight fails
    snapshotRequestDao.updateCreatedInfo(snapshotRequestId, null, null, null);
    return StepResult.getStepResultSuccess();
  }
}
