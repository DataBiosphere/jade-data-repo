package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class ConvertToPredictableFileIdsVerifyDatasetStep extends DefaultUndoStep {

  private final UUID datasetId;
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userReq;

  public ConvertToPredictableFileIdsVerifyDatasetStep(
      UUID datasetId, SnapshotService snapshotService, AuthenticatedUserRequest userReq) {
    this.datasetId = datasetId;
    this.snapshotService = snapshotService;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    long numSnapshotsFromDataset =
        snapshotService
            .enumerateSnapshots(
                userReq,
                0,
                1,
                EnumerateSortByParam.CREATED_DATE,
                SqlSortDirection.DESC,
                null,
                null,
                List.of(datasetId))
            .getFilteredTotal();
    if (numSnapshotsFromDataset > 0) {
      throw new IllegalArgumentException(
          "Dataset file ids cannot be migrated when a snapshot has been created from it");
    }
    return StepResult.getStepResultSuccess();
  }
}
