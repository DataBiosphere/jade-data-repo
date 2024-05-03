package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
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
import java.util.List;
import java.util.UUID;

public class CreateByQuerySnapshotRequestModelStep implements Step {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotDao snapshotDao;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final AuthenticatedUserRequest userReq;

  public CreateByQuerySnapshotRequestModelStep(
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

    String sqlString =
        snapshotBuilderService.generateRowIdQuery(accessRequest, dataReleaseSnapshot, userReq);

    // populate model with query and add to map
    SnapshotRequestModel snapshotRequestModel = new SnapshotRequestModel();
    snapshotRequestModel.name(accessRequest.getSnapshotName());
    // should this be the underlying dataset profile id? of the profile id on the snapshot?
    snapshotRequestModel.profileId(dataset.getDefaultProfileId());
    snapshotRequestModel.globalFileIds(true);
    // use underlying dataset to query
    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName(dataset.getName())
            .mode(SnapshotRequestContentsModel.ModeEnum.BYQUERY)
            .querySpec(new SnapshotRequestQueryModel().query(sqlString).assetName("person_visit"));
    snapshotRequestModel.contents(List.of(snapshotRequestContentsModel));
    // TODO: implement asset creation and time filtering
    context
        .getWorkingMap()
        .put(SnapshotWorkingMapKeys.BY_QUERY_SNAPSHOT_REQUEST_MODEL, snapshotRequestModel);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // nothing to undo
    return StepResult.getStepResultSuccess();
  }
}
