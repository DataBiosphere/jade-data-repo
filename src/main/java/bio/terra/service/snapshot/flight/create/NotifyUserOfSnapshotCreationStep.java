package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class NotifyUserOfSnapshotCreationStep extends DefaultUndoStep {
  private final AuthenticatedUserRequest userRequest;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final IamService iamService;
  private final UUID snapshotRequestId;

  public NotifyUserOfSnapshotCreationStep(
      AuthenticatedUserRequest userRequest,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      IamService iamService,
      UUID snapshotRequestId) {
    this.userRequest = userRequest;
    this.snapshotBuilderService = snapshotBuilderService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.iamService = iamService;
    this.snapshotRequestId = snapshotRequestId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    var request = snapshotRequestDao.getById(snapshotRequestId);
    var user = iamService.getUserIds(request.createdBy());
    snapshotBuilderService.notifySnapshotReady(
        userRequest, user.getUserSubjectId(), snapshotRequestId);
    return StepResult.getStepResultSuccess();
  }
}
