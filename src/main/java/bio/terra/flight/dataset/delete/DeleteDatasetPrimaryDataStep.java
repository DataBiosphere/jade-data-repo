package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.exception.NotFoundException;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.UUID;

public class DeleteDatasetPrimaryDataStep implements Step {


    private BigQueryPdao bigQueryPdao;
    private DatasetDao datasetDao;
    private FireStoreDependencyDao dependencyDao;
    private UUID datasetId;
    private StudyService studyService;

    public DeleteDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        DatasetDao datasetDao,
                                        FireStoreDependencyDao dependencyDao,
                                        UUID datasetId,
                                        StudyService studyService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetDao = datasetDao;
        this.dependencyDao = dependencyDao;
        this.datasetId = datasetId;
        this.studyService = studyService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Dataset dataset = datasetDao.retrieveDataset(datasetId);
            bigQueryPdao.deleteDataset(dataset);

            // Remove dataset file references from the underlying studies
            for (DatasetSource datasetSource : dataset.getDatasetSources()) {
                Study study = studyService.retrieve(datasetSource.getStudy().getId());
                dependencyDao.deleteDatasetFileDependencies(
                    study,
                    datasetId.toString());
            }

        } catch (NotFoundException nfe) {
            // If we do not find the study, we assume things are already clean
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // This step is not undoable. We only get here when the
        // metadata delete that comes after will has a dismal failure.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalStateException("Attempt to undo permanent delete"));
    }
}

