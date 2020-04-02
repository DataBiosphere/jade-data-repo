package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


public class DataDeletionStep implements Step {

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;

    private static Logger logger = LoggerFactory.getLogger(DataDeletionStep.class);

    public DataDeletionStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    private String suffix(FlightContext context) {
        return context.getFlightId().replace('-', '_');
    }

    private DataDeletionRequest request(FlightContext context) {
        return context.getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), DataDeletionRequest.class);
    }

    private Dataset dataset(FlightContext context) {
        String datasetId = context.getInputParameters()
            .get(JobMapKeys.DATASET_ID.getKeyName(), String.class);
        return datasetService.retrieve(UUID.fromString(datasetId));
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = dataset(context);
        String suffix = suffix(context);
        DataDeletionRequest dataDeletionRequest = request(context);

        // we want this soft delete operation to be atomic, we we will combine all of the inserts into one "thing" that
        // we send to bigquery.
        List<String> tableNames = dataDeletionRequest.getTables()
            .stream()
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());

        // the soft delete tables have a random suffix on them, we need to fetch those from the db and pass them in
        Map<String, String> sdTableNameLookup = dataset.getTables()
            .stream()
            .collect(Collectors.toMap(DatasetTable::getName, DatasetTable::getSoftDeleteTableName));

        bigQueryPdao.applySoftDeletes(dataset, tableNames, sdTableNameLookup, suffix);

        // TODO: this can be more informative, something like # rows deleted per table, or mismatched row ids
        DeleteResponseModel deleteResponseModel =
            new DeleteResponseModel().objectState(DeleteResponseModel.ObjectStateEnum.DELETED);
        FlightUtils.setResponse(context, deleteResponseModel, HttpStatus.OK);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The do step is atomic, either it worked or it didn't. There is no undo.
        return StepResult.getStepResultSuccess();
    }
}

