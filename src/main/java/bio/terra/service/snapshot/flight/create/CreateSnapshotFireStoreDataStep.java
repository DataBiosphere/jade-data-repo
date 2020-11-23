package bio.terra.service.snapshot.flight.create;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class CreateSnapshotFireStoreDataStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(CreateSnapshotFireStoreDataStep.class);

    private final BigQueryPdao bigQueryPdao;
    private final SnapshotService snapshotService;
    private final FireStoreDependencyDao dependencyDao;
    private final DatasetService datasetService;
    private final SnapshotRequestModel snapshotReq;
    private final FireStoreDao fileDao;
    private final PerformanceLogger performanceLogger;

    public CreateSnapshotFireStoreDataStep(BigQueryPdao bigQueryPdao,
                                           SnapshotService snapshotService,
                                           FireStoreDependencyDao dependencyDao,
                                           DatasetService datasetService,
                                           SnapshotRequestModel snapshotReq,
                                           FireStoreDao fileDao,
                                           PerformanceLogger performanceLogger) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotService = snapshotService;
        this.dependencyDao = dependencyDao;
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
        this.fileDao = fileDao;
        this.performanceLogger = performanceLogger;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        // We need a complete snapshot; use the snapshotService to get one.
        Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());

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

                        String bigQueryTimer = performanceLogger.timerStart();
                        List<String> refIds = bigQueryPdao.getSnapshotRefIds(snapshotSource.getDataset(),
                            snapshot.getName(),
                            mapTable.getFromTable().getName(),
                            mapTable.getFromTable().getId().toString(),
                            mapColumn.getFromColumn());
                        List<String> uniqueRefIds = refIds.stream().distinct().collect(Collectors.toList());
                        if (refIds.size() != uniqueRefIds.size()) {
                            logger.info("some files are repeated. {} unique values across {} total file references",
                                uniqueRefIds.size(), refIds.size());
                        }
                        performanceLogger.timerEndAndLog(
                            bigQueryTimer,
                            context.getFlightId(),
                            this.getClass().getName(),
                            "bigQueryPdao.getSnapshotRefIds",
                            refIds.size());

                        Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());

                        String addFilesTimer = performanceLogger.timerStart();
                        fileDao.addFilesToSnapshot(dataset, snapshot, refIds);
                        performanceLogger.timerEndAndLog(
                            addFilesTimer,
                            context.getFlightId(),
                            this.getClass().getName(),
                            "fileDao.addFilesToSnapshot",
                            refIds.size());

                        String addDependenciesTimer = performanceLogger.timerStart();
                        dependencyDao.storeSnapshotFileDependencies(dataset, snapshot.getId().toString(), uniqueRefIds);
                        performanceLogger.timerEndAndLog(
                            addDependenciesTimer,
                            context.getFlightId(),
                            this.getClass().getName(),
                            "dependencyDao.storeSnapshotFileDependencies",
                            refIds.size());
                    }
                }
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // Remove the snapshot file system and any file dependencies created
        Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
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

