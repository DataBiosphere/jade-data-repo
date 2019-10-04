package bio.terra.flight.snapshot.create;

import bio.terra.filedata.google.firestore.FireStoreDao;
import bio.terra.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.dataset.Dataset;
import bio.terra.snapshot.Snapshot;
import bio.terra.snapshot.SnapshotMapColumn;
import bio.terra.snapshot.SnapshotMapTable;
import bio.terra.snapshot.SnapshotSource;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.tabulardata.google.BigQueryPdao;
import bio.terra.dataset.DatasetService;
import bio.terra.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class CreateSnapshotFireStoreDataStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private SnapshotService snapshotService;
    private FireStoreDependencyDao dependencyDao;
    private DatasetService datasetService;
    private SnapshotRequestModel snapshotReq;
    private FireStoreDao fileDao;

    public CreateSnapshotFireStoreDataStep(BigQueryPdao bigQueryPdao,
                                           SnapshotService snapshotService,
                                           FireStoreDependencyDao dependencyDao,
                                           DatasetService datasetService,
                                           SnapshotRequestModel snapshotReq,
                                           FireStoreDao fileDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotService = snapshotService;
        this.dependencyDao = dependencyDao;
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We need a complete snapshot; use the snapshotService to get one.
        Snapshot snapshot = snapshotService.retrieveSnapshotByName(snapshotReq.getName());

        // Build the snapshot file system and record the file dependencies
        // The algorithm is:
        // Loop through sources, loop through map tables, loop through map columns
        // if from column is FILEREF or DIRREF, ask pdao to get the ids from that
        // column that are in the snapshot; do the filestore processing
        //
        // NOTE: This is brute force doing a column at a time. Depending on how much memory and
        // swap we want to use, we could extract all row ids in one go. Doing it column-by-column
        // bounds the intermediate size in a way.
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
                        fileDao.addFilesToSnapshot(dataset, snapshot, refIds);
                        dependencyDao.storeSnapshotFileDependencies(dataset, snapshot.getId().toString(), refIds);
                    }
                }
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Remove the snapshot file system and any file dependencies created
        Snapshot snapshot = snapshotService.retrieveSnapshotByName(snapshotReq.getName());
        fileDao.deleteFilesFromSnapshot(snapshot);
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());
            dependencyDao.deleteSnapshotFileDependencies(
                dataset,
                snapshot.getId().toString());
        }

        return StepResult.getStepResultSuccess();
    }

}

