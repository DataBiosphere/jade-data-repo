package bio.terra.service.snapshot.flight.create;

import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_FILE_ACCESS_FAULT;

import bio.terra.app.controller.exception.ApiException;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.storage.StorageException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotAuthzFileAclStep implements Step {
  private final FireStoreDependencyDao fireStoreDao;
  private final SnapshotService snapshotService;
  private final GcsPdao gcsPdao;
  private final DatasetService datasetService;
  private final ConfigurationService configService;
  private final UUID snapshotId;
  private static final Logger logger = LoggerFactory.getLogger(SnapshotAuthzFileAclStep.class);

  public SnapshotAuthzFileAclStep(
      FireStoreDependencyDao fireStoreDao,
      SnapshotService snapshotService,
      GcsPdao gcsPdao,
      DatasetService datasetService,
      ConfigurationService configService,
      UUID snapshotId) {
    this.fireStoreDao = fireStoreDao;
    this.snapshotService = snapshotService;
    this.gcsPdao = gcsPdao;
    this.datasetService = datasetService;
    this.configService = configService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    Map<IamRole, String> policies =
        workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, new TypeReference<>() {});

    // TODO: when we support multiple datasets, we can generate more than one copy of this
    //  step: one for each dataset. That is because each dataset keeps its file dependencies
    //  in its own scope. For now, we know there is exactly one dataset and we take shortcuts.

    SnapshotSource snapshotSource = snapshot.getFirstSnapshotSource();
    String datasetId = snapshotSource.getDataset().getId().toString();
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

    List<String> fileIds = fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
    try {
      if (configService.testInsertFault(SNAPSHOT_GRANT_FILE_ACCESS_FAULT)) {
        throw new StorageException(400, "Fake IAM failure", "badRequest", null);
      }

      gcsPdao.setAclOnFiles(dataset, fileIds, policies);
    } catch (StorageException ex) {
      // Now, how to figure out if the failure is due to IAM propagation delay. We know it will
      // be a 400 - bad request and the docs indicate the reason will be "badRequest". So for now
      // we will log alot and retry on that.
      // Note from DR-1760 - Potentially could remove this catch, should be handled with
      // ApiException below
      logger.error(
          "[SnapshotACLException] StorageException, potentially an ACL propagation error. "
              + "Message: {}, Reason: {}",
          ex.getMessage(),
          ex.getReason());
      if (ex.getCode() == 400 && (StringUtils.equals(ex.getReason(), "badRequest"))) {
        logger.error("[SnapshotACLException] Retrying! Bad Request.");
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw ex;
    } catch (ApiException ex) {
      // Most likely ACL propagation error
      // DR-1760 - Documents example of failure and successful retry
      logger.error(
          "[SnapshotACLException] ApiException. Message: {}, Cause: {}",
          ex.getMessage(),
          ex.getCause());
      if (ex.getCause().getMessage().contains("Could not find group")) {
        logger.error(
            "[SnapshotACLException] Retrying! 'Could not find group' exception - potentially"
                + "ACL propagation error.");
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw ex;
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    Map<IamRole, String> policies =
        workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, new TypeReference<>() {});

    // TODO: when we support multiple datasets, we can generate more than one copy of this
    //  step: one for each dataset. That is because each dataset keeps its file dependencies
    //  in its own scope. For now, we know there is exactly one dataset and we take shortcuts.

    SnapshotSource snapshotSource = snapshot.getFirstSnapshotSource();
    String datasetId = snapshotSource.getDataset().getId().toString();
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

    List<String> fileIds = fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
    try {
      gcsPdao.removeAclOnFiles(dataset, fileIds, policies);
    } catch (StorageException ex) {
      // We don't let the exception stop us from continuing to remove the rest of the snapshot
      // parts.
      // TODO: change this to whatever our alert-a-human log message is.
      logger.warn("NEEDS CLEANUP: Failed to remove snapshot reader ACLs from files", ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
