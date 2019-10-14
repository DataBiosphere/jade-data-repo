package bio.terra.service.snapshot.flight.create;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.stairway.FlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.util.List;

public class CreateSnapshotPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private SnapshotDao snapshotDao;
    private FireStoreDependencyDao dependencyDao;
    private DatasetService datasetService;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                         SnapshotDao snapshotDao,
                                         FireStoreDependencyDao dependencyDao,
                                         DatasetService datasetService,
                                         SnapshotRequestModel snapshotReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotDao = snapshotDao;
        this.dependencyDao = dependencyDao;
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        /*
         * map field ids into row ids and validate
         * then pass the row id array into create snapshot
         */
        SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);

        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        SnapshotSource source = snapshot.getSnapshotSources().get(0);
        RowIdMatch rowIdMatch = bigQueryPdao.mapValuesToRows(snapshot, source, contentsModel.getRootValues());
        if (rowIdMatch.getUnmatchedInputValues().size() != 0) {
            String unmatchedValues = String.join("', '", rowIdMatch.getUnmatchedInputValues());
            String message = String.format("Mismatched input values: '%s'", unmatchedValues);
            FlightUtils.setErrorResponse(context, message, HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new MismatchedValueException(message));
        }


        bigQueryPdao.createSnapshot(snapshot, rowIdMatch.getMatchingRowIds());

        // Add file references to the dependency table. The algorithm is:
        // Loop through sources, loop through map tables, loop through map columns
        // if from column is FILEREF or DIRREF, ask pdao to get the ids from that
        // column that are in the snapshot; tell file DAO to store them.
        //
        // NOTE: This is brute force doing a column at a time. Depending on how much memory and
        // swap we want to use, we could extract all row ids in one go. Doing it column-by-column
        // bounds the intermediate size in a way. I think this all becomes easier if we move
        // the filesystem stuff into DataStore or similar. Then bigquery can stream this
        // this without landing in memory and transferring it to postgres.
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            for (SnapshotMapTable mapTable : snapshotSource.getSnapshotMapTables()) {
                for (SnapshotMapColumn mapColumn : mapTable.getSnapshotMapColumns()) {
                    String fromDatatype = mapColumn.getFromColumn().getType();
                    if (StringUtils.equalsIgnoreCase(fromDatatype, "FILEREF") ||
                        StringUtils.equalsIgnoreCase(fromDatatype, "DIRREF")) {

                        List<String> refIds = bigQueryPdao.getSnapshotRefIds(snapshotSource.getDataset(),
                            snapshot.getName(),
                            mapTable.getFromTable().getName(),
                            mapTable.getFromTable().getId().toString(),
                            mapColumn.getFromColumn());

                        Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
                        dependencyDao.storeSnapshotFileDependencies(
                            dataset,
                            snapshot.getId().toString(),
                            refIds);
                    }
                }
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Remove any file dependencies created
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
            dependencyDao.deleteSnapshotFileDependencies(
                dataset,
                snapshot.getId().toString());
        }

        bigQueryPdao.deleteSnapshot(snapshot);
        return StepResult.getStepResultSuccess();
    }

}

