package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.transaction.TransactionSystemException;

public class CreateSnapshotCountTableRowsAzureStep implements Step {

  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;

  private static final Logger logger =
      LoggerFactory.getLogger(CreateSnapshotCountTableRowsAzureStep.class);

  public CreateSnapshotCountTableRowsAzureStep(
      SnapshotDao snapshotDao, SnapshotRequestModel snapshotReq) {
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    Map<String, Long> tableRowCounts =
        workingMap.get(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, HashMap.class);
    try {
      snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    } catch (PessimisticLockingFailureException | TransactionSystemException ex) {
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
