package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class SetParentOnSnapshotsStep implements Step {
  private final IamService iamService;
  private final UUID datasetId;
  private final AuthenticatedUserRequest userReq;
  private final boolean inheritSteward;

  public SetParentOnSnapshotsStep(
      IamService iamService,
      UUID datasetId,
      AuthenticatedUserRequest userReq,
      boolean inheritSteward) {
    this.iamService = iamService;
    this.datasetId = datasetId;
    this.userReq = userReq;
    this.inheritSteward = inheritSteward;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    manageParents(context, inheritSteward);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    manageParents(context, !inheritSteward);
    return StepResult.getStepResultSuccess();
  }

  private void manageParents(FlightContext context, boolean inherit) {
    List<UUID> snapshots =
        context.getWorkingMap().get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class);
    Objects.requireNonNull(snapshots)
        .forEach(
            snapshotId -> {
              if (inherit) {
                // do not catch and handle errors, if one occurs, fail the flight
                // we are not checking if a parent already exists because
                // we want to overwrite it no matter what it is
                iamService.setResourceParent(
                    userReq.getToken(),
                    IamResourceType.DATASNAPSHOT,
                    snapshotId,
                    IamResourceType.DATASET,
                    datasetId);
              } else {
                // we are not checking if the parent already exists or what it is because
                // these are only the children of the dataset
                iamService.deleteResourceParent(
                    userReq.getToken(), IamResourceType.DATASNAPSHOT, snapshotId);
              }
            });
  }
}
