package bio.terra.flight.study.delete;

import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.metadata.DatasetSummary;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;
import java.util.UUID;

public class DeleteStudyValidateStep implements Step {
    private DatasetDao datasetDao;
    private FireStoreDependencyDao dependencyDao;

    public DeleteStudyValidateStep(DatasetDao datasetDao, FireStoreDependencyDao dependencyDao) {
        this.datasetDao = datasetDao;
        this.dependencyDao = dependencyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID studyId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        List<DatasetSummary> datasets = datasetDao.retrieveDatasetsForStudy(studyId);
        if (datasets.size() != 0) {
            throw new ValidationException("Can not delete a study being used by datasets");
        }
        // Sanity check - validate that there are no stray file references. There should be none left
        // if there are no datasets returned from retrieveDatasetsForStudy.
        if (dependencyDao.studyHasDatasetReference(studyId.toString())) {
            throw new FileSystemCorruptException("File system has dataset dependencies; metadata does not");
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // no undo is possible
        return StepResult.getStepResultSuccess();
    }

}
