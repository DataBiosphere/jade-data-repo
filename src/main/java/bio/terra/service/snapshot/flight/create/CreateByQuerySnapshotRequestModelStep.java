package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderSettingsDao;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class CreateByQuerySnapshotRequestModelStep implements Step {
  private final SnapshotRequestModel snapshotReq;
  private final DatasetService datasetService;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private final SnapshotRequestDao snapshotRequestDao;
  private final AuthenticatedUserRequest userReq;

  public CreateByQuerySnapshotRequestModelStep(
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      SnapshotRequestDao snapshotRequestDao,
      AuthenticatedUserRequest userReq) {
    this.snapshotReq = snapshotReq;
    this.datasetService = datasetService;
    this.snapshotBuilderService = snapshotBuilderService;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.snapshotRequestDao = snapshotRequestDao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    UUID accessRequestId =
        snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
    SnapshotAccessRequestResponse accessRequest = snapshotRequestDao.getById(accessRequestId);

    // TODO: move to snapshot Id instead of dataset Id
    UUID dataReleaseDatasetId = accessRequest.getDatasetId();
    Dataset dataReleaseDataset = datasetService.retrieve(dataReleaseDatasetId);
    Instant filterByDate = dataReleaseDataset.getCreatedDate();
    SnapshotBuilderSettings settings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(dataReleaseDatasetId);

    List<List<SnapshotBuilderCriteriaGroup>> criteriaGroups =
        accessRequest.getSnapshotSpecification().getCohorts().stream()
            .map(SnapshotBuilderCohort::getCriteriaGroups)
            .toList();

    Query sqlQuery =
        new QueryBuilderFactory()
            .criteriaQueryBuilder("person", settings)
            .generateRowIdQueryForCriteriaGroupsList(criteriaGroups);
    String sqlString =
        sqlQuery.renderSQL(snapshotBuilderService.createContext(dataReleaseDataset, userReq));

    // populate model with query and add to map
    SnapshotRequestModel snapshotRequestModel = new SnapshotRequestModel();
    snapshotRequestModel.name(accessRequest.getSnapshotName());
    snapshotRequestModel.profileId(dataReleaseDataset.getDefaultProfileId());
    snapshotRequestModel.globalFileIds(true);
    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .mode(SnapshotRequestContentsModel.ModeEnum.BYQUERY)
            .querySpec(
                new SnapshotRequestQueryModel().query(sqlString).assetName("person_visit"));
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
