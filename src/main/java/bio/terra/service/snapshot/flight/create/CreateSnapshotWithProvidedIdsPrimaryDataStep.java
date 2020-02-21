package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotProvidedIdsRequestContentsModel;
import bio.terra.model.SnapshotProvidedIdsRequestModel;
import bio.terra.model.SnapshotProvidedIdsRequestTableModel;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightUtils;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.springframework.http.HttpStatus;

import java.util.List;

public class CreateSnapshotWithProvidedIdsPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private SnapshotDao snapshotDao;
    private SnapshotProvidedIdsRequestModel snapshotReq;

    public CreateSnapshotWithProvidedIdsPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                                        SnapshotDao snapshotDao,
                                                        SnapshotProvidedIdsRequestModel snapshotReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotDao = snapshotDao;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
        SnapshotProvidedIdsRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        SnapshotSource source = snapshot.getSnapshotSources().get(0);

        // for each table, make sure all of the row ids match
        for (SnapshotProvidedIdsRequestTableModel table : contentsModel.getTables()) {
            List<String> rowIds = table.getRowIds();
            if (!rowIds.isEmpty()) {
                RowIdMatch rowIdMatch = bigQueryPdao.matchRowIds(snapshot, source, table.getTableName(), rowIds);
                if (!rowIdMatch.getUnmatchedInputValues().isEmpty()) {
                    String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
                    String message = String.format("Mismatched row ids: '%s'", unmatchedValues);
                    FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
                    return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
                }
            }
        }
        bigQueryPdao.createSnapshotWithProvidedIds(snapshot, contentsModel);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Remove any file dependencies created
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        bigQueryPdao.deleteSnapshot(snapshot);
        return StepResult.getStepResultSuccess();
    }

}

