package bio.terra.flight.dataset.create;

import bio.terra.dao.DatasetDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.RowIdMatch;
import bio.terra.model.DatasetRequestContentsModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.DatasetService;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.util.List;

public class CreateDatasetPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;
    private DatasetDao datasetDao;
    private FireStoreDependencyDao dependencyDao;

    public CreateDatasetPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                        DatasetService datasetService,
                                        DatasetDao datasetDao,
                                        FireStoreDependencyDao dependencyDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.datasetDao = datasetDao;
        this.dependencyDao = dependencyDao;
    }

    DatasetRequestModel getRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);
    }

    Dataset getDataset(FlightContext context) {
        DatasetRequestModel datasetRequest = getRequestModel(context);
        return datasetService.makeDatasetFromDatasetRequest(datasetRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
         * map field ids into row ids and validate
         * then pass the row id array into create dataset
         */
        DatasetRequestModel requestModel = getRequestModel(context);
        DatasetRequestContentsModel contentsModel = requestModel.getContents().get(0);

        Dataset dataset = datasetDao.retrieveDatasetByName(requestModel.getName());
        DatasetSource source = dataset.getDatasetSources().get(0);
        RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(dataset, source, contentsModel.getRootValues());
        if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
            String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
            String message = String.format("Mismatched input values: '%s'", unmatchedValues);
            FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }

        bigQueryPdao.createDataset(dataset, rowIdMatch.getMatchingRowIds());

        // Add file references to the dependency table. The algorithm is:
        // Loop through sources, loop through map tables, loop through map columns
        // if from column is FILEREF or DIRREF, ask pdao to get the ids from that
        // column that are in the dataset; tell file DAO to store them.
        //
        // NOTE: This is brute force doing a column at a time. Depending on how much memory and
        // swap we want to use, we could extract all row ids in one go. Doing it column-by-column
        // bounds the intermediate size in a way. I think this all becomes easier if we move
        // the filesystem stuff into DataStore or similar. Then bigquery can stream this
        // this without landing in memory and transferring it to postgres.
        for (DatasetSource datasetSource : dataset.getDatasetSources()) {
            for (DatasetMapTable mapTable : datasetSource.getDatasetMapTables()) {
                for (DatasetMapColumn mapColumn : mapTable.getDatasetMapColumns()) {
                    String fromDatatype = mapColumn.getFromColumn().getType();
                    if (StringUtils.equalsIgnoreCase(fromDatatype, "FILEREF") ||
                        StringUtils.equalsIgnoreCase(fromDatatype, "DIRREF")) {

                        List<String> refIds = bigQueryPdao.getDatasetRefIds(datasetSource.getStudy(),
                            dataset.getName(),
                            mapTable.getFromTable().getName(),
                            mapTable.getFromTable().getId().toString(),
                            mapColumn.getFromColumn());

                        dependencyDao.storeDatasetFileDependencies(
                            datasetSource.getStudy().getId().toString(),
                            dataset.getId().toString(),
                            refIds);
                    }
                }
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteDataset(getDataset(context));
        return StepResult.getStepResultSuccess();
    }

}

