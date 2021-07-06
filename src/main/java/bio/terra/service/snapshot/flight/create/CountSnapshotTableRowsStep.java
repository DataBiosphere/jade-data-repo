package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;

public class CountSnapshotTableRowsStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;

  public CountSnapshotTableRowsStep(
      BigQueryPdao bigQueryPdao, SnapshotDao snapshotDao, SnapshotRequestModel snapshotReq) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    Map<String, Long> tableRowCounts = bigQueryPdao.getSnapshotTableRowCounts(snapshot);
    snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    // nothing to do here
    return StepResult.getStepResultSuccess();
  }
}
