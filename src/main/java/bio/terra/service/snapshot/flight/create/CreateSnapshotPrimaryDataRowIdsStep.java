package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateSnapshotPrimaryDataRowIdsStep implements Step {

  private BigQueryPdao bigQueryPdao;
  private SnapshotDao snapshotDao;
  private SnapshotService snapshotService;
  private SnapshotRequestModel snapshotReq;

  public CreateSnapshotPrimaryDataRowIdsStep(
      BigQueryPdao bigQueryPdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getFirstSnapshotSource();
    SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();
    Instant createdAt = CommonFlightUtils.getCreatedAt(context);

    // for each table, make sure all of the row ids match
    for (SnapshotRequestRowIdTableModel table : rowIdModel.getTables()) {
      List<UUID> rowIds = table.getRowIds();
      if (!rowIds.isEmpty()) {
        RowIdMatch rowIdMatch =
            bigQueryPdao.matchRowIds(source, table.getTableName(), rowIds, createdAt);
        if (!rowIdMatch.getUnmatchedInputValues().isEmpty()) {
          String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
          String message = String.format("Mismatched row ids: '%s'", unmatchedValues);
          FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
          return new StepResult(
              StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }
      }
    }
    bigQueryPdao.createSnapshotWithProvidedIds(snapshot, contentsModel, createdAt);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
