package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.time.Instant;

public record CreateSnapshotPrimaryDataFullViewGcpStep(
    BigQuerySnapshotPdao bigQuerySnapshotPdao,
    SnapshotDao snapshotDao,
    SnapshotService snapshotService,
    SnapshotRequestModel snapshotReq,
    Dataset sourceDataset)
    implements Step {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    /*
     * from the dataset tables, we will need to get the table's live views
     */
    Instant createdAt = CommonFlightUtils.getCreatedAt(context);
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    bigQuerySnapshotPdao.createSnapshotWithLiveViews(snapshot, sourceDataset, createdAt);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
