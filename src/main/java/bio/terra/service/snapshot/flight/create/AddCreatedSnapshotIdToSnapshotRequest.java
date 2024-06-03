package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddCreatedSnapshotIdToSnapshotRequest implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;
  private final UUID snapshotId;

  public AddCreatedSnapshotIdToSnapshotRequest(
      SnapshotRequestDao snapshotRequestDao, UUID snapshotRequestId, UUID snapshotId) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    snapshotRequestDao.updateCreatedSnapshotId(snapshotRequestId, snapshotId);
    return null;
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // remove the created snapshot id if the flight fails
    snapshotRequestDao.updateCreatedSnapshotId(snapshotRequestId, null);
    return null;
  }
}
