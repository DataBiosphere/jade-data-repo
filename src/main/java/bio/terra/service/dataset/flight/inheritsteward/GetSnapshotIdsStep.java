package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class GetSnapshotIdsStep extends DefaultUndoStep {
  private final SnapshotDao snapshotDao;
  private final IamService iamService;
  private final AuthenticatedUserRequest userReq;
  private final UUID datasetId;
  private final boolean inheritSteward;

  public GetSnapshotIdsStep(
      SnapshotDao snapshotDao,
      IamService iamService,
      AuthenticatedUserRequest userReq,
      UUID datasetId,
      boolean inheritSteward) {
    this.snapshotDao = snapshotDao;
    this.iamService = iamService;
    this.userReq = userReq;
    this.datasetId = datasetId;
    this.inheritSteward = inheritSteward;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<UUID> snapshotIds;
    if (inheritSteward) {
      snapshotIds = snapshotDao.getSnapshotIds(datasetId);
    } else {
      snapshotIds =
          iamService
              .listResourceChildren(
                  userReq.getToken(), bio.terra.service.auth.iam.IamResourceType.DATASET, datasetId)
              .stream()
              .map((child) -> UUID.fromString(child.getResourceId()))
              .collect(Collectors.toList());
    }
    context.getWorkingMap().put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    return StepResult.getStepResultSuccess();
  }
}
