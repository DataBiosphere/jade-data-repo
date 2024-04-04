package bio.terra.service.snapshot.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class DeleteSnapshotDeleteStorageAccountStep implements Step {

  private final UUID snapshotId;
  private final ResourceService resourceService;
  private final TableDao tableDao;
  private final ProfileService profileService;

  public DeleteSnapshotDeleteStorageAccountStep(
      UUID snapshotId,
      ResourceService resourceService,
      TableDao tableDao,
      ProfileService profileService) {
    this.snapshotId = snapshotId;
    this.resourceService = resourceService;
    this.tableDao = tableDao;
    this.profileService = profileService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();

    UUID profileId = workingMap.get(SnapshotWorkingMapKeys.PROFILE_ID, UUID.class);
    UUID storageResourceId =
        workingMap.get(SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_ID, UUID.class);

    BillingProfileModel snapshotBillingProfile = profileService.getProfileByIdNoCheck(profileId);

    AzureStorageAccountResource snapshotStorageAccountResource =
        resourceService.lookupStorageAccount(storageResourceId);

    AzureStorageAuthInfo snapshotAzureStorageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
            snapshotBillingProfile, snapshotStorageAccountResource);

    // If we do not find the storage account, we assume things are already clean
    if (snapshotStorageAccountResource == null) {
      // Setting this so that subsequent step that uses the resource name does not run
      workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, false);
      return StepResult.getStepResultSuccess();
    }

    workingMap.put(
        SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_NAME,
        snapshotStorageAccountResource.getName());
    workingMap.put(
        SnapshotWorkingMapKeys.STORAGE_ACCOUNT_RESOURCE_TLC,
        snapshotStorageAccountResource.getTopLevelContainer());

    resourceService.deleteStorageContainer(storageResourceId, profileId, context.getFlightId());

    // Delete the directory entries for the snapshot
    tableDao.deleteFilesFromSnapshot(snapshotAzureStorageAuthInfo, snapshotId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
