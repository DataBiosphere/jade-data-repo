package bio.terra.service.dataset.flight.datadelete;

import bio.terra.common.FlightUtils;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.stream.Collectors;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getSuffix;


public class DataDeletionStep implements Step {

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;

    private static Logger logger = LoggerFactory.getLogger(DataDeletionStep.class);

    public DataDeletionStep(BigQueryPdao bigQueryPdao, DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset dataset = getDataset(context, datasetService);
        String suffix = getSuffix(context);
        DataDeletionRequest dataDeletionRequest = getRequest(context);
        List<String> tableNames = dataDeletionRequest.getTables()
            .stream()
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toList());

        bigQueryPdao.validateDeleteRequest(dataset, dataDeletionRequest.getTables(), suffix);
        bigQueryPdao.applySoftDeletes(dataset, tableNames, suffix);

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

