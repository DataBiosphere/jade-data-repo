package bio.terra.service.snapshot.flight.create;

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
import com.google.cloud.storage.StorageException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_FILE_ACCESS_FAULT;

public class SnapshotAuthzFileAclStep implements Step {
    private final FireStoreDependencyDao fireStoreDao;
    private final SnapshotService snapshotService;
    private final GcsPdao gcsPdao;
    private final DatasetService datasetService;
    private final ConfigurationService configService;
    private static final Logger logger = LoggerFactory.getLogger(SnapshotAuthzFileAclStep.class);

    public SnapshotAuthzFileAclStep(FireStoreDependencyDao fireStoreDao,
                                    SnapshotService snapshotService,
                                    GcsPdao gcsPdao,
                                    DatasetService datasetService,
                                    ConfigurationService configService) {
        this.fireStoreDao = fireStoreDao;
        this.snapshotService = snapshotService;
        this.gcsPdao = gcsPdao;
        this.datasetService = datasetService;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        Snapshot snapshot = snapshotService.retrieve(snapshotId);

        String readersPolicyEmail = workingMap.get(SnapshotWorkingMapKeys.POLICY_EMAIL, String.class);

        // TODO: when we support multiple datasets, we can generate more than one copy of this
        //  step: one for each dataset. That is because each dataset keeps its file dependencies
        //  in its own scope. For now, we know there is exactly one dataset and we take shortcuts.

        SnapshotSource snapshotSource = snapshot.getSnapshotSources().get(0);
        String datasetId = snapshotSource.getDataset().getId().toString();
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        List<String> fileIds = fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
        try {
            if (configService.testInsertFault(SNAPSHOT_GRANT_FILE_ACCESS_FAULT)) {
                throw new StorageException(400, "Fake IAM failure", "badRequest", null);
            }

            gcsPdao.setAclOnFiles(dataset, fileIds, readersPolicyEmail);
        } catch (StorageException ex) {
            // Now, how to figure out if the failure is due to IAM propagation delay. We know it will
            // be a 400 - bad request and the docs indicate the reason will be "badRequest". So for now
            // we will log alot and retry on that.
            if (ex.getCode() == 400 && StringUtils.equals(ex.getReason(), "badRequest")) {
                logger.info("Maybe caught an ACL propagation error: " + ex.getMessage()
                    + " reason: " + ex.getReason(), ex);
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
            }
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        Snapshot snapshot = snapshotService.retrieve(snapshotId);

        String readersPolicyEmail = workingMap.get(SnapshotWorkingMapKeys.POLICY_EMAIL, String.class);

        // TODO: when we support multiple datasets, we can generate more than one copy of this
        //  step: one for each dataset. That is because each dataset keeps its file dependencies
        //  in its own scope. For now, we know there is exactly one dataset and we take shortcuts.

        SnapshotSource snapshotSource = snapshot.getSnapshotSources().get(0);
        String datasetId = snapshotSource.getDataset().getId().toString();
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));

        List<String> fileIds = fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
        try {
            gcsPdao.removeAclOnFiles(dataset, fileIds, readersPolicyEmail);
        } catch (StorageException ex) {
            // We don't let the exception stop us from continuing to remove the rest of the snapshot parts.
            // TODO: change this to whatever our alert-a-human log message is.
            logger.warn("NEEDS CLEANUP: Failed to remove snapshot reader ACLs from files", ex);
        }
        return StepResult.getStepResultSuccess();
    }
}
