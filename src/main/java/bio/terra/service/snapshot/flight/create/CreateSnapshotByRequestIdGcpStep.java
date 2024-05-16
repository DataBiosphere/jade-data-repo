package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.time.Instant;

public class CreateSnapshotByRequestIdGcpStep implements CreateSnapshotByRequestIdInterface, Step {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotService snapshotService;
  private final SnapshotBuilderService snapshotBuilderService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotDao snapshotDao;
  private final AuthenticatedUserRequest userReq;
  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;

  public CreateSnapshotByRequestIdGcpStep(
      SnapshotRequestModel snapshotReq,
      SnapshotService snapshotService,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq,
      BigQuerySnapshotPdao bigQuerySnapshotPdao) {
    this.snapshotReq = snapshotReq;
    this.snapshotService = snapshotService;
    this.snapshotBuilderService = snapshotBuilderService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotDao = snapshotDao;
    this.userReq = userReq;
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
  }

  @Override
  public StepResult createSnapshot(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException {
    bigQuerySnapshotPdao.queryForRowIds(assetSpecification, snapshot, sqlQuery, filterBefore);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    return prepareAndCreateSnapshot(
        context,
        snapshot,
        snapshotReq,
        snapshotBuilderService,
        snapshotRequestDao,
        snapshotDao,
        userReq);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
