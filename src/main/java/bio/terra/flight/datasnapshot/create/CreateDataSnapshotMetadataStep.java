package bio.terra.flight.datasnapshot.create;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.exception.NotFoundException;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.model.DataSnapshotRequestModel;
import bio.terra.model.DataSnapshotSummaryModel;
import bio.terra.service.DataSnapshotService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateDataSnapshotMetadataStep implements Step {
    private DataSnapshotDao dataSnapshotDao;
    private DataSnapshotService dataSnapshotService;

    public CreateDataSnapshotMetadataStep(DataSnapshotDao dataSnapshotDao, DataSnapshotService dataSnapshotService) {
        this.dataSnapshotDao = dataSnapshotDao;
        this.dataSnapshotService = dataSnapshotService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        DataSnapshotRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                DataSnapshotRequestModel.class);
        try {
            DataSnapshot dataSnapshot = dataSnapshotService.makeDataSnapshotFromDataSnapshotRequest(datasetRequest);
            UUID datasetId = dataSnapshotDao.create(dataSnapshot);
            context.getWorkingMap().put("datasetId", datasetId);
            DataSnapshotSummary dataSnapshotSummary = dataSnapshotDao.retrieveDataSnapshotSummary(dataSnapshot.getId());
            DataSnapshotSummaryModel response = dataSnapshotService.makeSummaryModelFromSummary(dataSnapshotSummary);
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
        DataSnapshotRequestModel datasetRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(),
                DataSnapshotRequestModel.class);
        String datasetName = datasetRequest.getName();
        dataSnapshotDao.deleteByName(datasetName);
        return StepResult.getStepResultSuccess();
    }

}
