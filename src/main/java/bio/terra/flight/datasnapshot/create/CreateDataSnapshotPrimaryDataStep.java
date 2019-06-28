package bio.terra.flight.datasnapshot.create;

import bio.terra.dao.DataSnapshotDao;
import bio.terra.filesystem.FireStoreDependencyDao;
import bio.terra.flight.FlightUtils;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotMapColumn;
import bio.terra.metadata.DataSnapshotMapTable;
import bio.terra.metadata.DataSnapshotSource;
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

public class CreateDataSnapshotPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;
    private DataSnapshotDao dataSnapshotDao;
    private FireStoreDependencyDao dependencyDao;

    public CreateDataSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                             DatasetService datasetService,
                                             DataSnapshotDao dataSnapshotDao,
                                             FireStoreDependencyDao dependencyDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.dataSnapshotDao = dataSnapshotDao;
        this.dependencyDao = dependencyDao;
    }

    DatasetRequestModel getRequestModel(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        return inputParameters.get(JobMapKeys.REQUEST.getKeyName(), DatasetRequestModel.class);
    }

    DataSnapshot getDataset(FlightContext context) {
        DatasetRequestModel datasetRequest = getRequestModel(context);
        return datasetService.makeDatasetFromDatasetRequest(datasetRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
         * map field ids into row ids and validate
         * then pass the row id array into create dataSnapshot
         */
        DatasetRequestModel requestModel = getRequestModel(context);
        DatasetRequestContentsModel contentsModel = requestModel.getContents().get(0);

        DataSnapshot dataSnapshot = dataSnapshotDao.retrieveDatasetByName(requestModel.getName());
        DataSnapshotSource source = dataSnapshot.getDataSnapshotSources().get(0);
        RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(dataSnapshot, source, contentsModel.getRootValues());
        if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
            String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
            String message = String.format("Mismatched input values: '%s'", unmatchedValues);
            FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }


        bigQueryPdao.createDataset(dataSnapshot, rowIdMatch.getMatchingRowIds());

        // Add file references to the dependency table. The algorithm is:
        // Loop through sources, loop through map tables, loop through map columns
        // if from column is FILEREF or DIRREF, ask pdao to get the ids from that
        // column that are in the data snapshot; tell file DAO to store them.
        //
        // NOTE: This is brute force doing a column at a time. Depending on how much memory and
        // swap we want to use, we could extract all row ids in one go. Doing it column-by-column
        // bounds the intermediate size in a way. I think this all becomes easier if we move
        // the filesystem stuff into DataStore or similar. Then bigquery can stream this
        // this without landing in memory and transferring it to postgres.
        for (DataSnapshotSource dataSnapshotSource : dataSnapshot.getDataSnapshotSources()) {
            for (DataSnapshotMapTable mapTable : dataSnapshotSource.getDataSnapshotMapTables()) {
                for (DataSnapshotMapColumn mapColumn : mapTable.getDataSnapshotMapColumns()) {
                    String fromDatatype = mapColumn.getFromColumn().getType();
                    if (StringUtils.equalsIgnoreCase(fromDatatype, "FILEREF") ||
                        StringUtils.equalsIgnoreCase(fromDatatype, "DIRREF")) {

                        List<String> refIds = bigQueryPdao.getDatasetRefIds(dataSnapshotSource.getStudy().getName(),
                            dataSnapshot.getName(),
                            mapTable.getFromTable().getName(),
                            mapTable.getFromTable().getId().toString(),
                            mapColumn.getFromColumn());

                        dependencyDao.storeDatasetFileDependencies(
                            dataSnapshotSource.getStudy().getId().toString(),
                            dataSnapshot.getId().toString(),
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

