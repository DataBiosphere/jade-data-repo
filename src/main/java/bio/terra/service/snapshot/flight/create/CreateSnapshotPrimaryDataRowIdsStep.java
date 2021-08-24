package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateSnapshotPrimaryDataRowIdsStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotReq;
  private final int sourceIndex;

  public CreateSnapshotPrimaryDataRowIdsStep(
      BigQueryPdao bigQueryPdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq,
      int sourceIndex) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
    this.sourceIndex = sourceIndex;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(sourceIndex);
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource snapshotSource = snapshot.getSnapshotSources().get(sourceIndex);
    SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();

    // for each table, make sure all of the row ids match
    for (SnapshotRequestRowIdTableModel table : rowIdModel.getTables()) {
      List<UUID> rowIds = table.getRowIds();
      if (!rowIds.isEmpty()) {
        RowIdMatch rowIdMatch =
            bigQueryPdao.matchRowIds(snapshotSource, table.getTableName(), rowIds);
        if (!rowIdMatch.getUnmatchedInputValues().isEmpty()) {
          String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
          String message = String.format("Mismatched row ids: '%s'", unmatchedValues);
          FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
          return new StepResult(
              StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }
      }
    }
    bigQueryPdao.createSnapshotWithProvidedIds(snapshotSource, contentsModel);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshotSource(snapshotReq.getName(), sourceIndex);
    return StepResult.getStepResultSuccess();
  }
}
