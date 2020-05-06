package bio.terra.service.filedata.flight.delete;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.exception.FileDependencyException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DeleteFileLookupStep implements Step {
    private static Logger logger = LoggerFactory.getLogger(DeleteFileLookupStep.class);

    private final FireStoreDao fileDao;
    private final String fileId;
    private final Dataset dataset;
    private final FireStoreDependencyDao dependencyDao;
    private final ConfigurationService configService;

    public DeleteFileLookupStep(FireStoreDao fileDao,
                                String fileId,
                                Dataset dataset,
                                FireStoreDependencyDao dependencyDao,
                                ConfigurationService configService) {
        this.fileDao = fileDao;
        this.fileId = fileId;
        this.dataset = dataset;
        this.dependencyDao = dependencyDao;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        if (configService.testInsertFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_STOP_FAULT)) {
            try {
                logger.info("FILE_DELETE_LOCK_CONFLICT_STOP_FAULT");
                while (!configService.testInsertFault(ConfigEnum.FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT)) {
                    logger.info("Sleeping for CONTINUE FAULT");
                    TimeUnit.SECONDS.sleep(5);
                }
                logger.info("FILE_DELETE_LOCK_CONFLICT_CONTINUE_FAULT");
            } catch (InterruptedException intEx) {
                Thread.currentThread().interrupt();
                throw new DatasetLockException("Unexpected interrupt during file delete lock fault", intEx);
            }
        }

        try {
            // If we are restarting, we may have already retrieved and saved the file,
            // so we check the working map before doing the lookup.
            FlightMap workingMap = context.getWorkingMap();
            FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
            if (fireStoreFile == null) {
                fireStoreFile = fileDao.lookupFile(dataset, fileId);
                if (fireStoreFile != null) {
                    workingMap.put(FileMapKeys.FIRESTORE_FILE, fireStoreFile);
                }
            }
            // We may not have found a fireStoreFile either way. We don't have a way to short-circuit
            // running the rest of the steps, so we use the null stored in the working map to let other
            // steps know there is no file. If there is a file, check dependencies here.
            if (fireStoreFile != null) {
                if (dependencyDao.fileHasSnapshotReference(dataset, fireStoreFile.getFileId())) {
                    throw new FileDependencyException(
                        "File is used by at least one snapshot and cannot be deleted");
                }
            }
        } catch (FileSystemAbortTransactionException rex) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }

}
