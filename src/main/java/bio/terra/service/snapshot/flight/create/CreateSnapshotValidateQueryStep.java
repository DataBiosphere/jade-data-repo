package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;

import bio.terra.grammar.Query;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Optional;

public class CreateSnapshotValidateQueryStep implements Step {

  private final DatasetService datasetService;
  private final SnapshotRequestModel snapshotReq;
  private final int sourceIndex;

  public CreateSnapshotValidateQueryStep(
      DatasetService datasetService, SnapshotRequestModel snapshotReq, int sourceIndex) {
    this.datasetService = datasetService;
    this.snapshotReq = snapshotReq;
    this.sourceIndex = sourceIndex;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    /*
     * make sure the query is valid
     * for now--this includes making sure there is only one dataset
     * passes general grammar check (will pass sql into parse method to make sure it works
     *
     * get dataset(s) from query and make sure that it exists-- initially just one and multiple in the future
     * make sure the user has custodian data access (currently this is done in the controller,
     * but this should be moved
     */
    String snapshotQuery = snapshotReq.getContents().get(sourceIndex).getQuerySpec().getQuery();
    Query query = Query.parse(snapshotQuery);
    List<String> datasetNames = query.getDatasetNames();
    if (datasetNames.isEmpty()) {
      return createFailResult("Snapshots much be associated with at least one dataset");
    }
    if (datasetNames.size() > 1) {
      return createFailResult("Snapshot queries can currently only use one dataset");
    }
    // Get the dataset by name to ensure the dataset exists
    String datasetName = datasetNames.get(0);
    // if not found, will throw a DatasetNotFoundException
    datasetService.retrieveByName(datasetName);

    List<String> tableNames = query.getTableNames();
    if (tableNames.isEmpty()) {
      return createFailResult("Snapshots must be associated with at least one table");
    }

    // TODO validate the select list. It should have one column that is the row id.
    List<String> columnNames = query.getColumnNames();
    if (columnNames.isEmpty()) {
      return createFailResult("Snapshots must be associated with at least one column");
    }
    Optional<String> rowId = columnNames.stream().filter(PDAO_ROW_ID_COLUMN::equals).findFirst();
    if (rowId.isEmpty()) {
      return createFailResult("Query must include a row_id column");
    }

    // TODO test this in an integration test
    return StepResult.getStepResultSuccess();
  }

  private static StepResult createFailResult(String message) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
