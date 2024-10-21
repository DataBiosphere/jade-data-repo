package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class AddFlightIdToSnapshotRequestStep implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;

  public AddFlightIdToSnapshotRequestStep(
      SnapshotRequestDao snapshotRequestDao, UUID snapshotRequestId) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    snapshotRequestDao.updateFlightId(snapshotRequestId, context.getFlightId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // we don't want to remove the flightId if the flight fails
    return StepResult.getStepResultSuccess();
  }
}
