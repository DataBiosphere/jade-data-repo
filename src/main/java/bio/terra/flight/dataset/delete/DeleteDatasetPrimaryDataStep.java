package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.exceptions.NotFoundException;
import bio.terra.metadata.Dataset;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.UUID;

public class DeleteDatasetPrimaryDataStep implements Step {


    private BigQueryPdao bigQueryPdao;
    private DatasetDao datasetDao;
    private UUID datasetId;

    public DeleteDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        DatasetDao datasetDao,
                                        UUID datasetId) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Dataset dataset = datasetDao.retrieveDataset(datasetId);
            bigQueryPdao.deleteDataset(dataset);
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

