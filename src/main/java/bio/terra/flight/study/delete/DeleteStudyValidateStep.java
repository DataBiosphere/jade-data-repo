package bio.terra.flight.study.delete;

import bio.terra.controller.exception.ValidationException;
import bio.terra.dao.DataSnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.metadata.DataSnapshotSummary;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;
import java.util.UUID;

public class DeleteStudyValidateStep implements Step {
    private DataSnapshotDao dataSnapshotDao;
    private FireStoreDependencyDao dependencyDao;

    public DeleteStudyValidateStep(DataSnapshotDao dataSnapshotDao, FireStoreDependencyDao dependencyDao) {
        this.dataSnapshotDao = dataSnapshotDao;
        this.dependencyDao = dependencyDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID studyId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        List<DataSnapshotSummary> datasets = dataSnapshotDao.retrieveDatasetsForStudy(studyId);
        if (datasets.size() != 0) {
            throw new ValidationException("Can not delete a study being used by datasets");
        }
        // Sanity check - validate that there are no stray file references. There should be none left
        // if there are no data snapshots returned from retrieveDatasetsForStudy.
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
