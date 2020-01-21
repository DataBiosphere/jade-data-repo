package bio.terra.service.snapshot.flight.create;


import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotRequestContainer;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateSnapshotFireStoreComputeStep implements Step {

    private SnapshotService snapshotService;
    private SnapshotRequestContainer snapshotRequestContainer;
    private FireStoreDao fileDao;

    public CreateSnapshotFireStoreComputeStep(SnapshotService snapshotService,
                                              SnapshotRequestContainer snapshotRequestContainer,
                                              FireStoreDao fileDao) {
        this.snapshotService = snapshotService;
        this.snapshotRequestContainer = snapshotRequestContainer;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Snapshot snapshot = snapshotService.retrieveByName(snapshotRequestContainer.getName());
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

