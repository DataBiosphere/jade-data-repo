package bio.terra.flight.snapshot.create;

import bio.terra.dao.SnapshotDao;
import bio.terra.filesystem.FireStoreDao;
import bio.terra.metadata.Snapshot;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateSnapshotFireStoreComputeStep implements Step {

    private SnapshotDao snapshotDao;
    private SnapshotRequestModel snapshotReq;
    private FireStoreDao fileDao;

    public CreateSnapshotFireStoreComputeStep(SnapshotDao snapshotDao,
                                              SnapshotRequestModel snapshotReq,
                                              FireStoreDao fileDao) {
        this.snapshotDao = snapshotDao;
        this.snapshotReq = snapshotReq;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        fileDao.snapshotCompute(snapshot);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo - if we are undoing all the way, the whole snapshot file system will get
        // torn down.
        return StepResult.getStepResultSuccess();
    }

}

