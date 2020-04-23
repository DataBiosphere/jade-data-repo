package bio.terra.service.snapshot.flight.create;

import bio.terra.grammar.Query;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.List;

public class CreateSnapshotValidateQueryStep implements Step {

    private DatasetService datasetService;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotValidateQueryStep(DatasetService datasetService,
                                           SnapshotRequestModel snapshotReq) {
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
        * make sure the query is valid
        * for now--this includes making sure there is only one dataset
        * passes general grammar check (will pass sql into parse method to make sure it works
        *
        * get dataset(s) from query and make sure that it exists-- initially just one, but multiple in the future
        * make sure the user has custodian data access (currently this is done in the controller, but this should be moved
        */
        String snapshotQuery = snapshotReq.getContents().get(0).getQuerySpec().getQuery();
        Query query = Query.parse(snapshotQuery);
        List<String> datasetNames = query.getDatasetNames();
        if (datasetNames.isEmpty()) {
            String message = String.format("Snapshots much be associated with at least one dataset");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }
        if (datasetNames.size() > 1) {
            String message = String.format("Snapshots can currently only be associated with one dataset");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }
        String datasetName = datasetNames.get(0);

        // TODO this will throw DatasetNotFoundException
        Dataset dataset = datasetService.retrieveByName(datasetName);

        if (datasetNames.size() > 1) {
            String message = String.format("Snapshots can currently only be associated with one dataset");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }

        List<String> tableNames = query.getTableNames();
        if (tableNames.isEmpty()) {
            String message = String.format("Snapshots much be associated with at least one table");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }

        // TODO validate the select list. It should have one column that is the row id.
        List<String> columnNames = query.getColumnNames();
        if (columnNames.isEmpty()) {
            String message = String.format("Snapshots much be associated with at least one column");
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }


        // TODO test this in an integration test

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

