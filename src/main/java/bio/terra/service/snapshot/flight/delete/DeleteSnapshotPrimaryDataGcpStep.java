package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotPrimaryDataGcpStep implements Step {

  private static Logger logger = LoggerFactory.getLogger(DeleteSnapshotPrimaryDataGcpStep.class);

  private BigQueryPdao bigQueryPdao;
  private SnapshotService snapshotService;
  private FireStoreDao fileDao;
  private UUID snapshotId;
  private ConfigurationService configService;

  public DeleteSnapshotPrimaryDataGcpStep(
      BigQueryPdao bigQueryPdao,
      SnapshotService snapshotService,
      FireStoreDao fileDao,
      UUID snapshotId,
      ConfigurationService configService) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotService = snapshotService;
    this.fileDao = fileDao;
    this.snapshotId = snapshotId;
    this.configService = configService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    try {
      // this fault is used by the SnapshotConnectedTest > testOverlappingDeletes
      if (configService.testInsertFault(ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
        logger.info("SNAPSHOT_DELETE_LOCK_CONFLICT_STOP_FAULT");
        while (!configService.testInsertFault(
            ConfigEnum.SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
          logger.info("Sleeping for CONTINUE FAULT");
          TimeUnit.SECONDS.sleep(5);
        }
        logger.info("SNAPSHOT_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
      }

      Snapshot snapshot = snapshotService.retrieve(snapshotId);
      if (snapshot.getProjectResource().getGoogleProjectId() != null) {
        bigQueryPdao.deleteSnapshot(snapshot);
        fileDao.deleteFilesFromSnapshot(snapshot);
      } else {
        logger.info("Google project Id is null, so assume this this an Azure project.");
      }

    } catch (BigQueryException ex) {
      if (FlightUtils.isBigQueryIamPropagationError(ex)) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw new PdaoException("Caught BQ exception while deleting snapshot", ex);
    } catch (SnapshotNotFoundException | DatasetNotFoundException nfe) {
      // If we do not find the snapshot or dataset, we assume things are already clean
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // This step is not undoable. We only get here when the
    // metadata delete that comes after will has a dismal failure.
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
