package bio.terra.flight.study.delete;

import bio.terra.controller.exception.ValidationException;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class DeleteStudyValidateStep implements Step {
    private FireStoreDependencyDao dependencyDao;

    public DeleteStudyValidateStep(FireStoreDependencyDao dependencyDao) {
        this.dependencyDao = dependencyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID studyId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        if (dependencyDao.studyHasDatasetReference(studyId.toString())) {
            throw new ValidationException("Can not delete study being used by datasets");
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // no undo is possible
        return StepResult.getStepResultSuccess();
    }

}
