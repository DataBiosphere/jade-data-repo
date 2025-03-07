package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;

public class SetParentOnSnapshotsStep implements Step {
  private final SnapshotService snapshotService;
  private final IamService iamService;
  private final UUID datasetId;
  private final AuthenticatedUserRequest userReq;

  public SetParentOnSnapshotsStep(
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
    String accessToken = userReq.getToken();
    List<UUID> snapshots =
        context.getWorkingMap().get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class);
    Objects.requireNonNull(snapshots)
        .forEach(
            snapshotId ->
                // do not catch and handle errors, if one occurs, fail the flight
                // we are not checking if a parent already exists because
                // we want to overwrite it no matter what it is
                iamService.setResourceParent(
                    accessToken,
                    IamResourceType.DATASNAPSHOT,
                    snapshotId,
                    IamResourceType.DATASET,
                    datasetId));
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    List<FullyQualifiedResourceId> children =
        iamService.listResourceChildren(userReq.getToken(), IamResourceType.DATASET, datasetId);
    children.stream()
        .filter(
            child ->
                child
                    .getResourceTypeName()
                    .equalsIgnoreCase(IamResourceType.DATASNAPSHOT.getSamResourceName()))
        .forEach(
            child -> {
              iamService.deleteResourceParent(
                  userReq.getToken(),
                  IamResourceType.DATASNAPSHOT,
                  UUID.fromString(child.getResourceId()));
            });
    ;
    return StepResult.getStepResultSuccess();
  }
}
