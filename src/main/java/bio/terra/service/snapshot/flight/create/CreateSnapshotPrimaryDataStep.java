package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.common.FlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

public class CreateSnapshotPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private SnapshotDao snapshotDao;
    private FireStoreDependencyDao dependencyDao;
    private DatasetService datasetService;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                         SnapshotDao snapshotDao,
                                         FireStoreDependencyDao dependencyDao,
                                         DatasetService datasetService,
                                         SnapshotRequestModel snapshotReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotDao = snapshotDao;
        this.dependencyDao = dependencyDao;
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
         * map field ids into row ids and validate
         * then pass the row id array into create snapshot
         */
        SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);

        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        SnapshotSource source = snapshot.getSnapshotSources().get(0);
        RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(snapshot, source, contentsModel.getRootValues());
        if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
            String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
            String message = String.format("Mismatched input values: '%s'", unmatchedValues);
            FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }

        bigQueryPdao.createSnapshot(snapshot, rowIdMatch.getMatchingRowIds());

        // REVIEWERS: There used to be a block of code here for updating FireStore with dependency info. I *think*
        // this is currently handled by CreateSnapshotFireStoreDataStep, so I am removing it from this step.

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Remove any file dependencies created
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
            dependencyDao.deleteSnapshotFileDependencies(
                dataset,
                snapshot.getId().toString());
        }

        bigQueryPdao.deleteSnapshot(snapshot);
        return StepResult.getStepResultSuccess();
    }

}

