package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteOutstandingSnapshotAccessRequestsStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteOutstandingSnapshotAccessRequestsStep.class);
  private final SnapshotBuilderService snapshotBuilderService;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest userReq;

  public DeleteOutstandingSnapshotAccessRequestsStep(
      AuthenticatedUserRequest userReq,
      UUID snapshotId,
      SnapshotBuilderService snapshotBuilderService) {
    this.snapshotId = snapshotId;
    this.userReq = userReq;
    this.snapshotBuilderService = snapshotBuilderService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    try {
      EnumerateSnapshotAccessRequest requestResponseList =
          snapshotBuilderService.enumerateRequestsBySnapshot(snapshotId);
      requestResponseList
          .getItems()
          .forEach(
              snapshotAccessRequestResponse ->
                  snapshotBuilderService.deleteRequest(
                      userReq, snapshotAccessRequestResponse.getId()));
    } catch (NotFoundException e) {
      // Do nothing, if there are no requests we are good.
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // can't undo delete
    logger.warn(
        String.format(
            "Cannot undo delete resource for snapshot access requests on snapshot %s", snapshotId));
    return null;
  }
}
