package bio.terra.service.snapshot.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
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
import java.util.UUID;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotDependencyDataAzureStep extends OptionalStep {
  private static Logger logger =
      LoggerFactory.getLogger(DeleteSnapshotDependencyDataAzureStep.class);

  private TableDependencyDao tableDependencyDao;
  private UUID snapshotId;
  private DatasetService datasetService;
  private ProfileService profileService;
  private ResourceService resourceService;
  private AzureAuthService azureAuthService;

  public DeleteSnapshotDependencyDataAzureStep(
      TableDependencyDao tableDependencyDao,
      UUID snapshotId,
      DatasetService datasetService,
      ProfileService profileService,
      ResourceService resourceService,
      AzureAuthService azureAuthService,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.tableDependencyDao = tableDependencyDao;
    this.snapshotId = snapshotId;
    this.datasetService = datasetService;
    this.profileService = profileService;
    this.resourceService = resourceService;
    this.azureAuthService = azureAuthService;
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
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
    tableDependencyDao.deleteSnapshotFileDependencies(
        datasetTableServiceClient, dataset.getId(), snapshotId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return new StepResult(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        new IllegalStateException("Attempt to undo permanent delete"));
  }
}
