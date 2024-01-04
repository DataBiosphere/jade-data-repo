package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.transaction.TransactionSystemException;

public class CountSnapshotTableRowsStep implements Step {

  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;

  private static final Logger logger = LoggerFactory.getLogger(CountSnapshotTableRowsStep.class);

  public CountSnapshotTableRowsStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      SnapshotDao snapshotDao,
      SnapshotRequestModel snapshotReq) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    Map<String, Long> tableRowCounts = bigQuerySnapshotPdao.getSnapshotTableRowCounts(snapshot);
    try {
      snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    } catch (CannotSerializeTransactionException | TransactionSystemException ex) {
      logger.error("Could not serialize the transaction. Retrying.", ex);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    // nothing to do here
    return StepResult.getStepResultSuccess();
  }
}
