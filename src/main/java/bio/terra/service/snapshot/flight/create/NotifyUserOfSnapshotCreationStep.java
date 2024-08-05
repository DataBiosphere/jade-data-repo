package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class NotifyUserOfSnapshotCreationStep extends DefaultUndoStep {
  private final SnapshotBuilderService snapshotBuilderService;
  private final AuthenticatedUserRequest user;
  private final UUID snapshotRequestId;

  public NotifyUserOfSnapshotCreationStep(
      SnapshotBuilderService snapshotBuilderService,
      AuthenticatedUserRequest user,
      UUID snapshotRequestId) {
    this.snapshotBuilderService = snapshotBuilderService;
    this.user = user;
    this.snapshotRequestId = snapshotRequestId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    snapshotBuilderService.notifySnapshotReady(user, snapshotRequestId);
    return StepResult.getStepResultSuccess();
  }
}
