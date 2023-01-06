package bio.terra.service.snapshot.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableTransactionFailedException;
import java.util.UUID;

public class DeleteSnapshotDependencyDataAzureStep extends DefaultUndoStep {

  private final TableDependencyDao tableDependencyDao;
  private final UUID snapshotId;
  private final DatasetService datasetService;
  private final ProfileService profileService;
  private final ResourceService resourceService;
  private final AzureAuthService azureAuthService;

  public DeleteSnapshotDependencyDataAzureStep(
      TableDependencyDao tableDependencyDao,
      UUID snapshotId,
      DatasetService datasetService,
      ProfileService profileService,
      ResourceService resourceService,
      AzureAuthService azureAuthService) {
    this.tableDependencyDao = tableDependencyDao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
    this.profileService = profileService;
    this.resourceService = resourceService;
    this.azureAuthService = azureAuthService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    UUID datasetId = map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
    Dataset dataset = datasetService.retrieve(datasetId);

    BillingProfileModel datasetBillingProfile =
        profileService.getProfileByIdNoCheck(dataset.getDefaultProfileId());
    AzureStorageAccountResource datasetStorageAccountResource =
        resourceService.getDatasetStorageAccount(dataset, datasetBillingProfile);
    AzureStorageAuthInfo datasetAzureStorageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
            datasetBillingProfile, datasetStorageAccountResource);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetAzureStorageAuthInfo);
    try {
      tableDependencyDao.deleteSnapshotFileDependencies(
          datasetTableServiceClient, dataset.getId(), snapshotId);
    } catch (TableTransactionFailedException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }

    return StepResult.getStepResultSuccess();
  }
}
