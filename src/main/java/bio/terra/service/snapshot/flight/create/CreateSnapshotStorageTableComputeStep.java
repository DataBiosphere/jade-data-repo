package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateSnapshotStorageTableComputeStep implements Step {

  //  private SnapshotService snapshotService;
  //  private SnapshotRequestModel snapshotReq;
  //  private FireStoreDao fileDao;

  public CreateSnapshotStorageTableComputeStep(
      SnapshotService snapshotService, SnapshotRequestModel snapshotReq, FireStoreDao fileDao) {
    //    this.snapshotService = snapshotService;
    //    this.snapshotReq = snapshotReq;
    //    this.fileDao = fileDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    // TODO - replace w/  tableDao.snapshotCompute
    // fileDao.snapshotCompute(snapshot);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // No undo - if we are undoing all the way, the whole snapshot file system will get
    // torn down.
    return StepResult.getStepResultSuccess();
  }
}
