package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.snapshot.SnapshotDao;
import com.azure.data.tables.TableServiceClient;
import java.util.UUID;

public class DeleteDatasetAzureValidateStep extends DeleteDatasetValidateStep {
  private TableDependencyDao tableDependencyDao;
  private AzureAuthService azureAuthService;

  public DeleteDatasetAzureValidateStep(SnapshotDao snapshotDao,
      FireStoreDependencyDao dependencyDao, DatasetService datasetService, UUID datasetId) {
    super(snapshotDao, dependencyDao, datasetService, datasetId);
  }

  @Override
  boolean hasSnapshotReference(Dataset dataset)
      throws InterruptedException {
    // pull from sm context
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(
            storageAuthInfo.getSubscriptionId(),
            storageAuthInfo.getResourceGroupName(),
            storageAuthInfo.getStorageAccountResourceName());
    return tableDependencyDao.datasetHasSnapshotReference(tableServiceClient, dataset.getId());
  }
}
