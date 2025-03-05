package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class InheritStewardSetParentOnSnapshotsStep implements Step {
  private final SnapshotService snapshotService;
  private final IamService iamService;
  private final UUID datasetId;
  private final AuthenticatedUserRequest userReq;

  public InheritStewardSetParentOnSnapshotsStep(
      SnapshotService snapshotService,
      IamService iamService,
      UUID datasetId,
      AuthenticatedUserRequest userReq) {
    this.snapshotService = snapshotService;
    this.iamService = iamService;
    this.datasetId = datasetId;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<UUID> snapshots = snapshotService.enumerateSnapshotIdsForDataset(datasetId, userReq);
    snapshots.forEach(
        snapshotId -> {
          // do not catch and handle errors, if one occurs, fail the flight
          // we are not checking if a parent already exists because
          // we want to overwrite it no matter what it is
          iamService.setResourceParent(
              userReq.getToken(),
              IamResourceType.DATASNAPSHOT,
              snapshotId,
              IamResourceType.DATASET,
              datasetId);
        });
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    List<UUID> snapshots = snapshotService.enumerateSnapshotIdsForDataset(datasetId, userReq);
    snapshots.forEach(
        snapshotId -> {
          // we are not checking to see if the parent is the dataset set in the doStep
          // because we want to delete all parents here regardless of what they are
          try {
            iamService.deleteResourceParent(
                userReq.getToken(), IamResourceType.DATASNAPSHOT, snapshotId);
          } catch (NotFoundException e) {
            // if a snapshot does not have a parent, continue
          }
        });
    return StepResult.getStepResultSuccess();
  }
}
