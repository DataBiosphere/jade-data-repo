package bio.terra.flight.snapshot.create;

import bio.terra.dao.SnapshotDao;
import bio.terra.exception.NotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.SnapshotSummary;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.SnapshotService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateSnapshotMetadataStep implements Step {
    private SnapshotDao snapshotDao;
    private SnapshotService snapshotService;

    public CreateSnapshotMetadataStep(SnapshotDao snapshotDao, SnapshotService snapshotService) {
        this.snapshotDao = snapshotDao;
        this.snapshotService = snapshotService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        SnapshotRequestModel snapshotRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                SnapshotRequestModel.class);
        try {
            Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequest);
            UUID snapshotId = snapshotDao.create(snapshot);
            context.getWorkingMap().put("snapshotId", snapshotId);
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
        FlightMap inputParameters = context.getInputParameters();
        SnapshotRequestModel snapshotRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                SnapshotRequestModel.class);
        String snapshotName = snapshotRequest.getName();
        snapshotDao.deleteByName(snapshotName);
        return StepResult.getStepResultSuccess();
    }

}
