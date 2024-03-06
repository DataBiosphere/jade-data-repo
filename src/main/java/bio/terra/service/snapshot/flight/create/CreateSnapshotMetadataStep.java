package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.flight.duos.SnapshotDuosFlightUtils;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.TransactionSystemException;

public class CreateSnapshotMetadataStep implements Step {
  private final SnapshotDao snapshotDao;
  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotReq;

  private static final Logger logger = LoggerFactory.getLogger(CreateSnapshotMetadataStep.class);

  public CreateSnapshotMetadataStep(
      SnapshotDao snapshotDao, SnapshotService snapshotService, SnapshotRequestModel snapshotReq) {
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    try {
      FlightMap workingMap = context.getWorkingMap();
      // fill in the ideas that we made in previous steps
      UUID projectResourceId =
          workingMap.get(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, UUID.class);
      UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
      Snapshot snapshot =
          snapshotService
              .makeSnapshotFromSnapshotRequest(snapshotReq)
              .id(snapshotId)
              .projectResourceId(projectResourceId);
      if (snapshotReq.getDuosId() != null) {
        DuosFirecloudGroupModel duosFirecloudGroup =
            SnapshotDuosFlightUtils.getFirecloudGroup(context);
        UUID duosFirecloudGroupId =
            SnapshotDuosFlightUtils.getDuosFirecloudGroupId(duosFirecloudGroup);
        snapshot.duosFirecloudGroupId(duosFirecloudGroupId);
      }
      snapshotDao.createAndLock(snapshot, context.getFlightId());
      return StepResult.getStepResultSuccess();
    } catch (InvalidSnapshotException isEx) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, isEx);
    } catch (SnapshotNotFoundException ex) {
      FlightUtils.setErrorResponse(context, ex.toString(), HttpStatus.BAD_REQUEST);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    } catch (PessimisticLockingFailureException | TransactionSystemException ex) {
      logger.error("Could not serialize the transaction. Retrying.", ex);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    logger.debug("Snapshot creation failed. Deleting metadata.");
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    snapshotDao.delete(snapshotId);
    return StepResult.getStepResultSuccess();
  }
}
