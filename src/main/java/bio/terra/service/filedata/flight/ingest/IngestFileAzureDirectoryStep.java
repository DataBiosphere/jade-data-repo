package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.models.TableServiceException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileAzureDirectoryStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(IngestFileDirectoryStep.class);

  private final TableDao tableDao;
  private final FileMetadataUtils fileMetadataUtils;
  private final Dataset dataset;

  public IngestFileAzureDirectoryStep(
      TableDao tableDao, FileMetadataUtils fileMetadataUtils, Dataset dataset) {
    this.tableDao = tableDao;
    this.fileMetadataUtils = fileMetadataUtils;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel loadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    FlightMap workingMap = context.getWorkingMap();
    String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    workingMap.put(FileMapKeys.LOAD_COMPLETED, false);

    String datasetId = dataset.getId().toString();
    String targetPath = loadModel.getTargetPath();

    String ingestFileAction = workingMap.get(FileMapKeys.INGEST_FILE_ACTION, String.class);
    AzureStorageAccountResource storageAccountResource =
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    try {
      // The state logic goes like this:
      //  1. the directory entry doesn't exist. We need to create the directory entry for it.
      //  2. the directory entry exists and the load tags match:
      //      a. directory entry loadTag matches our loadTag AND entry fileId matches our fileId:
      //         means we are recovering and need to complete the file creation work.
      //      b. directory entry loadTag matches our loadTag AND entry fileId does NOT match our
      // fileId
      //         means this is a re-run of a load job. We update the fileId in the working map. We
      // don't
      //         know if we are recovering or already finished. We try to retrieve the file object
      // for
      //         the entry fileId:
      //           i. If that is successful, then we already loaded this file. We store
      // "completed=true"
      //              in the working map, so other steps do nothing.
      //          ii. If that fails, then we are recovering: we leave completed unset (=false) in
      // the working map.
      //
      // Lookup the file - on a recovery, we may have already created it, but not
      // finished. Or it might already exist, created by someone else.
      FireStoreDirectoryEntry existingEntry =
          tableDao.lookupDirectoryEntryByPath(
              dataset, targetPath, billingProfileModel, storageAccountResource);
      if (ingestFileAction.equals(ValidateIngestFileDirectoryStep.CREATE_ENTRY_ACTION)) {
        // (1) Not there - create it
        FireStoreDirectoryEntry newEntry =
            new FireStoreDirectoryEntry()
                .fileId(fileId)
                .isFileRef(true)
                .path(fileMetadataUtils.getDirectoryPath(loadModel.getTargetPath()))
                .name(fileMetadataUtils.getName(loadModel.getTargetPath()))
                .datasetId(datasetId)
                .loadTag(loadModel.getLoadTag());
        tableDao.createDirectoryEntry(newEntry, billingProfileModel, storageAccountResource);
      } else if (ingestFileAction.equals(ValidateIngestFileDirectoryStep.CHECK_ENTRY_ACTION)
          && !StringUtils.equals(existingEntry.getFileId(), fileId)) {
        // (b) We are in a re-run of a load job. Try to get the file entry.
        fileId = existingEntry.getFileId();
        workingMap.put(FileMapKeys.FILE_ID, fileId);
        FireStoreFile fileEntry =
            tableDao.lookupFile(fileId, billingProfileModel, storageAccountResource);
        if (fileEntry != null) {
          // (b)(i) We successfully loaded this file already
          workingMap.put(FileMapKeys.LOAD_COMPLETED, true);
        }
        // (b)(ii) We are recovering and should continue this load; leave load completed false/unset

      }
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    String ingestFileAction = workingMap.get(FileMapKeys.INGEST_FILE_ACTION, String.class);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccountResource =
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);

    if (ingestFileAction.equals(ValidateIngestFileDirectoryStep.CREATE_ENTRY_ACTION)) {
      try {
        tableDao.deleteDirectoryEntry(fileId, billingProfileModel, storageAccountResource);
      } catch (TableServiceException rex) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
