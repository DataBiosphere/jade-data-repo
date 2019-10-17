package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.common.exception.NotFoundException;
import bio.terra.stairway.FlightUtils;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.snapshot.SnapshotService;
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
