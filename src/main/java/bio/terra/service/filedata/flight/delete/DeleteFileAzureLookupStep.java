package bio.terra.service.filedata.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class DeleteFileAzureLookupStep extends DefaultUndoStep {

  private final TableDao tableDao;
  private final String fileId;
  private final Dataset dataset;
  private final ResourceService resourceService;
  private final ProfileDao profileDao;

  public DeleteFileAzureLookupStep(
      TableDao tableDao,
      String fileId,
      Dataset dataset,
      ResourceService resourceService,
      ProfileDao profileDao) {
    this.tableDao = tableDao;
    this.fileId = fileId;
    this.dataset = dataset;
    this.resourceService = resourceService;
    this.profileDao = profileDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    try {
      // If we are restarting, we may have already retrieved and saved the file,
      // so we check the working map before doing the lookup.
      FlightMap workingMap = context.getWorkingMap();
      FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
      BillingProfileModel billingProfile =
          profileDao.getBillingProfileById(dataset.getDefaultProfileId());
      AzureStorageAccountResource storageAccountResource =
          resourceService.getOrCreateDatasetStorageAccount(
              dataset, billingProfile, context.getFlightId());

      AzureStorageAuthInfo storageAuthInfo =
          AzureStorageAuthInfo.azureStorageAuthInfoBuilder(billingProfile, storageAccountResource);
      workingMap.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, storageAuthInfo);

      if (fireStoreFile == null) {
        fireStoreFile = tableDao.lookupFile(dataset.getId().toString(), fileId, storageAuthInfo);
        if (fireStoreFile != null) {
          workingMap.put(FileMapKeys.FIRESTORE_FILE, fireStoreFile);
        }
      }
      // TODO: Do this check when Azure snapshots are implemented
      // We may not have found a fireStoreFile either way. We don't have a way to short-circuit
      // running the rest of the steps, so we use the null stored in the working map to let other
      // steps know there is no file. If there is a file, check dependencies here.
      //      if (fireStoreFile != null) {
      //        if (dependencyDao.fileHasSnapshotReference(dataset, fireStoreFile.getFileId())) {
      //          throw new FileDependencyException(
      //              "File is used by at least one snapshot and cannot be deleted");
      //        }
      //      }
    } catch (FileSystemAbortTransactionException rex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, rex);
    }

    return StepResult.getStepResultSuccess();
  }
}
