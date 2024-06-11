package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.*;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.UUID;

public interface CreateSnapshotByRequestIdInterface {
  default StepResult prepareAndCreateSnapshot(
      FlightContext context,
      Snapshot snapshot,
      SnapshotRequestModel snapshotReq,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq)
      throws InterruptedException {
    var workingMap = context.getWorkingMap();
    UUID accessRequestId =
        snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
    SnapshotAccessRequestResponse accessRequest = snapshotRequestDao.getById(accessRequestId);

    UUID sourceSnapshotId = accessRequest.getSourceSnapshotId();
    Snapshot sourceSnapshot = snapshotDao.retrieveSnapshot(sourceSnapshotId);

    AssetSpecification assetSpecification =
        workingMap.get(
            SnapshotWorkingMapKeys.SNAPSHOT_ASSET_SPECIFICATION, AssetSpecification.class);
    String sqlQuery =
        snapshotBuilderService.generateRowIdQuery(accessRequest, sourceSnapshot, userReq);
    Instant createdAt = sourceSnapshot.getCreatedDate();
    return createSnapshot(context, assetSpecification, snapshot, sqlQuery, createdAt);
  }

  StepResult createSnapshot(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException;
}
