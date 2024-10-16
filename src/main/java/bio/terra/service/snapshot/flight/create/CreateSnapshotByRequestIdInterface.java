package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.SnapshotAccessRequestModel;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.UUID;

public interface CreateSnapshotByRequestIdInterface {
  default StepResult prepareAndCreateSnapshot(
      FlightContext context,
      Snapshot snapshot,
      SnapshotRequestModel snapshotReq,
      SnapshotService snapshotService,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq)
      throws InterruptedException {
    UUID accessRequestId =
        snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
    SnapshotAccessRequestModel accessRequest =
        snapshotService.getSnapshotAccessRequestById(accessRequestId);

    UUID sourceSnapshotId = accessRequest.sourceSnapshotId();
    Snapshot sourceSnapshot = snapshotDao.retrieveSnapshot(sourceSnapshotId);
    Dataset dataset = sourceSnapshot.getSourceDataset();

    AssetSpecification queryAssetSpecification =
        snapshotService.buildAssetFromSnapshotAccessRequest(dataset, accessRequest);
    String sqlQuery =
        snapshotBuilderService.generateRowIdQuery(accessRequest, sourceSnapshot, userReq);
    Instant createdAt = sourceSnapshot.getCreatedDate();
    return createSnapshot(context, queryAssetSpecification, snapshot, sqlQuery, createdAt);
  }

  StepResult createSnapshot(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException;
}
