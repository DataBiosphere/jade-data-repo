package bio.terra.service.snapshot.flight;

import bio.terra.common.FlightUtils;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class LockSnapshotStep implements Step {

  private SnapshotDao snapshotDao;
  private UUID snapshotId;

  private static Logger logger = LoggerFactory.getLogger(LockSnapshotStep.class);

  public LockSnapshotStep(SnapshotDao snapshotDao, UUID snapshotId) {
    this.snapshotDao = snapshotDao;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    try {
      snapshotDao.lock(snapshotId, context.getFlightId());

      return StepResult.getStepResultSuccess();
    } catch (SnapshotLockException lockedEx) {
      logger.debug("Another flight has already locked this Snapshot", lockedEx);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, lockedEx);
    } catch (SnapshotNotFoundException notFoundEx) {
      DeleteResponseModel.ObjectStateEnum stateEnum =
          bio.terra.model.DeleteResponseModel.ObjectStateEnum.NOT_FOUND;
      DeleteResponseModel deleteResponseModel = new DeleteResponseModel().objectState(stateEnum);
      FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.NOT_FOUND);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, notFoundEx);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // try to unlock the flight if something went wrong above
    // note the unlock will only clear the flightid if it's set to this flightid
    boolean rowUpdated = snapshotDao.unlock(snapshotId, context.getFlightId());
    logger.debug("rowUpdated on unlock = " + rowUpdated);

    return StepResult.getStepResultSuccess();
  }
}
