package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteDatasetMetadataStep implements Step {

    private DatasetDao datasetDao;
    private UUID datasetId;

    public DeleteDatasetMetadataStep(DatasetDao datasetDao, UUID datasetId) {
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String datasetName = workingMap.get("datasetName", String.class);
        if (datasetName != null) {
            // There was a dataset when we looked. Try the delete
            datasetDao.delete(datasetId);
        }

        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // This step is not undoable. We only get here when the
        // do method has a dismal failure.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalStateException("Attempt to undo permanent delete"));
    }
}

