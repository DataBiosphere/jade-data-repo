package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class GetSnapshotIdsStep extends DefaultUndoStep {
  private final SnapshotService snapshotService;
  private final IamService iamService;
  private final AuthenticatedUserRequest userReq;
  private final UUID datasetId;
  private final boolean inheritSteward;

  public GetSnapshotIdsStep(
      SnapshotService snapshotService,
      IamService iamService,
      AuthenticatedUserRequest userReq,
      UUID datasetId,
      boolean inheritSteward) {
    this.snapshotService = snapshotService;
    this.iamService = iamService;
    this.userReq = userReq;
    this.datasetId = datasetId;
    this.inheritSteward = inheritSteward;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<UUID> snapshotIds;
    if (inheritSteward) {
      snapshotIds = new ArrayList<>(snapshotService.enumerateSnapshotIdsForDataset(datasetId, userReq));
    } else {
      snapshotIds =
          iamService
              .listResourceChildren(userReq.getToken(), IamResourceType.DATASET, datasetId)
              .stream()
              .filter(
                  child ->
                      child
                          .getResourceTypeName()
                          .equals(IamResourceType.DATASNAPSHOT.getSamResourceName()))
              .map(snapshot -> UUID.fromString(snapshot.getResourceId()))
              .collect(Collectors.toList());
    }
    context.getWorkingMap().put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    return StepResult.getStepResultSuccess();
  }
}
