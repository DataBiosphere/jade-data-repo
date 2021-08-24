package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
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
import org.springframework.http.HttpStatus;

public class CreateSnapshotPrimaryDataAssetStep implements Step {

  private final BigQueryPdao bigQueryPdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotReq;
  private final int sourceIndex;

  public CreateSnapshotPrimaryDataAssetStep(
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
    /*
     * map field ids into row ids and validate
     * then pass the row id array into create snapshot
     */
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(sourceIndex);
    SnapshotRequestAssetModel assetSpec = contentsModel.getAssetSpec();

    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource snapshotSource = snapshot.getSnapshotSources().get(sourceIndex);
    RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(snapshotSource, assetSpec.getRootValues());
    if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
      String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
      String message = String.format("Mismatched input values: '%s'", unmatchedValues);
      FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
    }

    bigQueryPdao.createSnapshot(snapshotSource, rowIdMatch.getMatchingRowIds());

    // REVIEWERS: There used to be a block of code here for updating FireStore with dependency info.
    // I *think*
    // this is currently handled by CreateSnapshotFireStoreDataStep, so I am removing it from this
    // step.

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshotSource(snapshotReq.getName(), sourceIndex);
    return StepResult.getStepResultSuccess();
  }
}
