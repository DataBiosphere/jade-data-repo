package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public class AddFlightIdToSnapshotRequest implements Step {
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;
  public AddFlightIdToSnapshotRequest(SnapshotRequestDao snapshotRequestDao, @NotNull @Valid UUID snapshotRequestId) {
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
