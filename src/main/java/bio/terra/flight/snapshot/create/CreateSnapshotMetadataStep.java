package bio.terra.flight.snapshot.create;

import bio.terra.snapshot.dao.SnapshotDao;
import bio.terra.exception.NotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.flight.snapshot.SnapshotWorkingMapKeys;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.SnapshotSummary;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.snapshot.service.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateSnapshotMetadataStep implements Step {
    private SnapshotDao snapshotDao;
    private SnapshotService snapshotService;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotMetadataStep(
        SnapshotDao snapshotDao, SnapshotService snapshotService, SnapshotRequestModel snapshotReq) {
        this.snapshotDao = snapshotDao;
        this.snapshotService = snapshotService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotReq);
            UUID snapshotId = snapshotDao.create(snapshot);
            context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_ID, snapshotId);
            SnapshotSummary snapshotSummary = snapshotDao.retrieveSnapshotSummary(snapshot.getId());
            SnapshotSummaryModel response = snapshotService.makeSummaryModelFromSummary(snapshotSummary);
            FlightUtils.setResponse(context, response, HttpStatus.CREATED);
            return StepResult.getStepResultSuccess();
        } catch (NotFoundException ex) {
            FlightUtils.setErrorResponse(context, ex.toString(), HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        String snapshotName = snapshotReq.getName();
        snapshotDao.deleteByName(snapshotName);
        return StepResult.getStepResultSuccess();
    }

}
