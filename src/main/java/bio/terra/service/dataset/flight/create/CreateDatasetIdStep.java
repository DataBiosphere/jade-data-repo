package bio.terra.service.dataset.flight.create;

import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class CreateDatasetIdStep implements Step {

    private DatasetRequestModel datasetRequest;

    private static Logger logger = LoggerFactory.getLogger(CreateDatasetIdStep.class);

    public CreateDatasetIdStep(DatasetRequestModel datasetRequest) {
        this.datasetRequest = datasetRequest;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // creates the dataset id and puts it in the working map

        try {
            FlightMap workingMap = context.getWorkingMap();
            UUID datasetId = UUID.randomUUID();
            workingMap.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);
            return StepResult.getStepResultSuccess();
        } catch (InvalidDatasetException idEx) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, idEx);
        } catch (Exception ex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new InvalidDatasetException("Cannot create dataset: " + datasetRequest.getName(), ex));
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        logger.debug("Dataset creation failed during id creation.");
        return StepResult.getStepResultSuccess();
    }
}

