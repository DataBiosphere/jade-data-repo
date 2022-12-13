package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotAuthzResource extends DefaultUndoStep {
  private final IamService sam;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest userReq;
  private static final Logger logger = LoggerFactory.getLogger(DeleteSnapshotAuthzResource.class);

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
}
