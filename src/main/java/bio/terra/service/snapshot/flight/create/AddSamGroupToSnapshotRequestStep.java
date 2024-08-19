package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddSamGroupToSnapshotRequestStep implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;
  private final AuthenticatedUserRequest userReq;

  public AddSamGroupToSnapshotRequestStep(
      SnapshotRequestDao snapshotRequestDao,
      UUID snapshotRequestId,
      AuthenticatedUserRequest userReq) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    String samGroupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);
    String samGroupEmail =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class);
    String createdBy = userReq.getEmail();
    if (samGroupName == null || samGroupEmail == null || createdBy == null) {
      throw new IllegalArgumentException(
          "Missing required sam group information. Group name, email, or requester is missing.");
    }
    snapshotRequestDao.updateSamGroup(snapshotRequestId, samGroupName, samGroupEmail, createdBy);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotRequestDao.updateSamGroup(snapshotRequestId, null, null, null);
    return StepResult.getStepResultSuccess();
  }
}
