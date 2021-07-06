package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.NotFoundException;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotAuthzResource implements Step {
  private IamService sam;
  private UUID snapshotId;
  private AuthenticatedUserRequest userReq;
  private static Logger logger = LoggerFactory.getLogger(DeleteSnapshotAuthzResource.class);

  public DeleteSnapshotAuthzResource(
      IamService sam, UUID snapshotId, AuthenticatedUserRequest userReq) {
    this.sam = sam;
    this.snapshotId = snapshotId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    try {
      sam.deleteSnapshotResource(userReq, snapshotId);
    } catch (NotFoundException ex) {
      // If we can't find it consider the delete successful.
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    logger.warn("Trying to undo delete resource for snapshot " + snapshotId.toString());
    return StepResult.getStepResultSuccess();
  }
}
