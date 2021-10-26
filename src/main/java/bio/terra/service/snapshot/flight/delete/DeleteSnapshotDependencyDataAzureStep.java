package bio.terra.service.snapshot.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.TableServiceClient;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotDependencyDataAzureStep implements Step {
  private static Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotDependencyDataAzureStep.class);

  private TableDependencyDao tableDependencyDao;
  private UUID snapshotId;
  private DatasetService datasetService;
  private ProfileService profileService;
  private ResourceService resourceService;
  private AzureAuthService azureAuthService;
  private UUID datasetId;

  public DeleteSnapshotDependencyDataAzureStep(
      TableDependencyDao tableDependencyDao,
      UUID snapshotId,
      DatasetService datasetService,
      ProfileService profileService,
      ResourceService resourceService,
      AzureAuthService azureAuthService,
      UUID datasetId) {
    this.tableDependencyDao = tableDependencyDao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
    this.profileService = profileService;
    this.resourceService = resourceService;
    this.azureAuthService = azureAuthService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(datasetId);
    try {
      BillingProfileModel datasetBillingProfile =
          profileService.getProfileByIdNoCheck(dataset.getDefaultProfileId());
      AzureStorageAccountResource datasetStorageAccountResource =
          resourceService.getDatasetStorageAccount(dataset, datasetBillingProfile);
      AzureStorageAuthInfo datasetAzureStorageAuthInfo =
          AzureStorageAuthInfo.azureStorageAuthInfoBuilder(
              datasetBillingProfile, datasetStorageAccountResource);
      TableServiceClient datasetTableServiceClient =
          azureAuthService.getTableServiceClient(datasetAzureStorageAuthInfo);
      tableDependencyDao.deleteSnapshotFileDependencies(
          datasetTableServiceClient, dataset.getId(), snapshotId);
    } catch (CorruptMetadataException ex) {
      logger.info(
          "No storage account found for dataset, so assume this is actually a GCP project.");
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
