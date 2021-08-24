package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateSnapshotPrimaryDataFullViewStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotReq;
  private final int sourceIndex;

  public CreateSnapshotPrimaryDataFullViewStep(
      BigQueryPdao bigQueryPdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      int sourceIndex) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.sourceIndex = sourceIndex;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    /*
     * from the dataset tables, we will need to get the table's live views
     */
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getSnapshotSources().get(sourceIndex);
    bigQueryPdao.createSnapshotWithLiveViews(source);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshotSource(snapshotReq.getName(), sourceIndex);
    // FIXME - multi-source
    // See BigQueryPDao.snapshotCreateBQDataset() - need to remove dataset
    // bigQueryProject.deleteDataset(snapshotName);
    return StepResult.getStepResultSuccess();
  }
}
