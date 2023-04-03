package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public record CreateSnapshotFireStoreComputeStep(
    SnapshotService snapshotService, SnapshotRequestModel snapshotReq, FireStoreDao fileDao)
    implements Step {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    try {
      fileDao.snapshotCompute(snapshot);
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // No undo - if we are undoing all the way, the whole snapshot file system will get
    // torn down.
    return StepResult.getStepResultSuccess();
  }
}
