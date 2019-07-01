package bio.terra.flight.datasnapshot.delete;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.exception.NotFoundException;
import bio.terra.metadata.DataSnapshot;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.UUID;

public class DeleteDataSnapshotPrimaryDataStep implements Step {


    private BigQueryPdao bigQueryPdao;
    private DataSnapshotDao dataSnapshotDao;
    private UUID datasetId;

    public DeleteDataSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                             DataSnapshotDao dataSnapshotDao,
                                             UUID datasetId) {
        this.bigQueryPdao = bigQueryPdao;
        this.dataSnapshotDao = dataSnapshotDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            DataSnapshot dataSnapshot = dataSnapshotDao.retrieveDataSnapshot(datasetId);
            bigQueryPdao.deleteDataSnapshot(dataSnapshot);
        } catch (NotFoundException nfe) {
            // If we do not find the dataset, we assume things are already clean
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

