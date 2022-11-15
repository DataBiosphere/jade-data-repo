package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class CreateSnapshotPrimaryDataAssetGcpStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateSnapshotPrimaryDataAssetGcpStep.class);

  private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private SnapshotDao snapshotDao;
  private SnapshotService snapshotService;
  private SnapshotRequestModel snapshotReq;

  public CreateSnapshotPrimaryDataAssetGcpStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotDao = snapshotDao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    /*
     * map field ids into row ids and validate
     * then pass the row id array into create snapshot
     */
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestAssetModel assetSpec = contentsModel.getAssetSpec();

    // TODO - remove
    ObjectMapper mapper = new ObjectMapper();
    try {
      String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(assetSpec);
      logger.info("[assetTest] SnapshotRequestAssetModel: " + prettyJson);
    } catch (JsonProcessingException ex) {
      logger.info("[assetTest] " + ex);
    }

    Instant createdAt = CommonFlightUtils.getCreatedAt(context);

    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getFirstSnapshotSource();

    // instead of a query, it looks like we're basically building a query to match
    // the input values.
    RowIdMatch rowIdMatch =
        bigQuerySnapshotPdao.mapValuesToRows(source, assetSpec.getRootValues(), createdAt);
    if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
      String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
      String message = String.format("Mismatched input values: '%s'", unmatchedValues);
      FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
    }

    bigQuerySnapshotPdao.createSnapshot(snapshot, rowIdMatch.getMatchingRowIds(), createdAt);

    // REVIEWERS: There used to be a block of code here for updating FireStore with dependency info.
    // I *think*
    // this is currently handled by CreateSnapshotFireStoreDataStep, so I am removing it from this
    // step.

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
