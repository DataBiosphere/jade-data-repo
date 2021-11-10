package bio.terra.service.dataset.flight.delete;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.data.tables.TableServiceClient;
import java.util.UUID;

public class DeleteDatasetAzureValidateStep extends DeleteDatasetValidateStep {
  private TableDependencyDao tableDependencyDao;
  private AzureAuthService azureAuthService;
  private ProfileDao profileDao;
  private final ResourceService resourceService;

  public DeleteDatasetAzureValidateStep(
      SnapshotDao snapshotDao,
      FireStoreDependencyDao dependencyDao,
      DatasetService datasetService,
      UUID datasetId,
      TableDependencyDao tableDependencyDao,
      AzureAuthService azureAuthService,
      ProfileDao profileDao,
      ResourceService resourceService) {
    super(snapshotDao, dependencyDao, datasetService, datasetId);
    this.tableDependencyDao = tableDependencyDao;
    this.azureAuthService = azureAuthService;
    this.profileDao = profileDao;
    this.resourceService = resourceService;
  }

  @Override
  boolean hasSnapshotReference(Dataset dataset, FlightContext context) throws InterruptedException {
    BillingProfileModel profileModel =
        profileDao.getBillingProfileById(dataset.getDefaultProfileId());
    AzureStorageAccountResource storageAccountResource =
        resourceService.getDatasetStorageAccount(dataset, profileModel);
    AzureStorageAuthInfo storageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(profileModel, storageAccountResource);

    FlightMap map = context.getWorkingMap();
    map.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, storageAuthInfo);

    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    return tableDependencyDao.datasetHasSnapshotReference(tableServiceClient, dataset.getId());
  }
}
