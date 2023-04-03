package bio.terra.service.snapshot.flight.create;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record CreateSnapshotFireStoreDataStep(
    BigQuerySnapshotPdao bigQuerySnapshotPdao,
    SnapshotService snapshotService,
    FireStoreDependencyDao dependencyDao,
    DatasetService datasetService,
    SnapshotRequestModel snapshotReq,
    FireStoreDao fileDao,
    PerformanceLogger performanceLogger)
    implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(CreateSnapshotFireStoreDataStep.class);

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
    // NOTE: This is brute force doing a column at a time. We extract all fileIds that need to be
    // processed to
    // ensure that we don't reprocess the same file repeatedly since this can have sever performance
    // impacts
    // TODO: We may want to find a more memory efficient way to track this
    for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
      Set<String> uniqueRefIds = new HashSet<>();
      int numFilesSeen = 0;
      for (SnapshotMapTable mapTable : snapshotSource.getSnapshotMapTables()) {
        for (SnapshotMapColumn mapColumn : mapTable.getSnapshotMapColumns()) {
          TableDataType fromDatatype = mapColumn.getFromColumn().getType();
          if (fromDatatype == TableDataType.FILEREF || fromDatatype == TableDataType.DIRREF) {

            String bigQueryTimer = performanceLogger.timerStart();
            List<String> refIds =
                bigQuerySnapshotPdao.getSnapshotRefIds(
                    snapshotSource.getDataset(),
                    snapshot,
                    mapTable.getFromTable().getName(),
                    mapTable.getFromTable().getId().toString(),
                    mapColumn.getFromColumn());
            numFilesSeen += refIds.size();
            uniqueRefIds.addAll(refIds);
            performanceLogger.timerEndAndLog(
                bigQueryTimer,
                context.getFlightId(),
                this.getClass().getName(),
                "bigQueryPdao.getSnapshotRefIds",
                refIds.size());
          }
        }
      }

      if (numFilesSeen != uniqueRefIds.size()) {
        logger.info(
            "some files are repeated. {} unique values across {} total file references",
            uniqueRefIds.size(),
            numFilesSeen);
      }

      List<String> uniqueRefIdsAsList = new ArrayList<>(uniqueRefIds);
      Dataset dataset = datasetService.retrieve(snapshotSource.getDataset().getId());

      String addFilesTimer = performanceLogger.timerStart();
      fileDao.addFilesToSnapshot(dataset, snapshot, uniqueRefIdsAsList);
      performanceLogger.timerEndAndLog(
          addFilesTimer,
          context.getFlightId(),
          this.getClass().getName(),
          "fileDao.addFilesToSnapshot",
          uniqueRefIds.size());

      String addDependenciesTimer = performanceLogger.timerStart();
      dependencyDao.storeSnapshotFileDependencies(
          dataset, snapshot.getId().toString(), uniqueRefIdsAsList);
      performanceLogger.timerEndAndLog(
          addDependenciesTimer,
          context.getFlightId(),
          this.getClass().getName(),
          "dependencyDao.storeSnapshotFileDependencies",
          uniqueRefIds.size());
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
      dependencyDao.deleteSnapshotFileDependencies(dataset, snapshot.getId().toString());
    }

    return StepResult.getStepResultSuccess();
  }
}
