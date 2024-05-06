package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class ConvertSnapshotRequestModelStep implements Step {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotDao snapshotDao;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final AuthenticatedUserRequest userReq;

  public ConvertSnapshotRequestModelStep(
      SnapshotRequestModel snapshotReq,
      SnapshotDao snapshotDao,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      AuthenticatedUserRequest userReq) {
    this.snapshotReq = snapshotReq;
    this.snapshotDao = snapshotDao;
    this.snapshotBuilderService = snapshotBuilderService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    UUID accessRequestId =
        snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
    SnapshotAccessRequestResponse accessRequest = snapshotRequestDao.getById(accessRequestId);

    UUID dataReleaseSnapshotId = accessRequest.getSourceSnapshotId();
    Snapshot dataReleaseSnapshot = snapshotDao.retrieveSnapshot(dataReleaseSnapshotId);
    // get the underlying dataset for the snapshot
    if (dataReleaseSnapshot.getSnapshotSources().isEmpty()) {
      throw new IllegalArgumentException("Snapshot does not have a source dataset");
    }
    Dataset dataset = dataReleaseSnapshot.getSnapshotSources().get(0).getDataset();
    // gets pre-existing asset on the dataset
    // TODO: create custom asset on dataset / save custom asset in working map
    AssetSpecification asset = dataset.getAssetSpecificationByName("person_visit").orElseThrow();
    String sqlString = snapshotBuilderService.generateRowIdQuery(accessRequest, dataset, userReq);

    context.getWorkingMap().put(SnapshotWorkingMapKeys.SQL_QUERY, sqlString);
    context.getWorkingMap().put(SnapshotWorkingMapKeys.ASSET, asset);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // nothing to undo
    return StepResult.getStepResultSuccess();
  }
}
