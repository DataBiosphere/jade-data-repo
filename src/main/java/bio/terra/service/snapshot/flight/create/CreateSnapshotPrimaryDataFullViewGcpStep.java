package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.time.Instant;

public class CreateSnapshotPrimaryDataFullViewGcpStep implements Step {

  private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private DatasetService datasetservice;
  private SnapshotDao snapshotDao;
  private SnapshotService snapshotService;
  private SnapshotRequestModel snapshotReq;

  public CreateSnapshotPrimaryDataFullViewGcpStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      DatasetService datasetservice,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.datasetservice = datasetservice;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    /*
     * from the dataset tables, we will need to get the table's live views
     */
    Instant createdAt = CommonFlightUtils.getCreatedAt(context);
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    Dataset dataset = datasetservice.retrieveByName(contentsModel.getDatasetName());
    bigQuerySnapshotPdao.createSnapshotWithLiveViews(snapshot, dataset, createdAt);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
