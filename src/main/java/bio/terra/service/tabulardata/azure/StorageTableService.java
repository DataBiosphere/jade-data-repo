package bio.terra.service.tabulardata.azure;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.data.tables.TableServiceClient;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageTableService {
  private final AzureStorageTablePdao storageTableDao;
  private final AzureAuthService azureAuthService;
  private final ResourceService resourceService;

  @Autowired
  public StorageTableService(
      AzureStorageTablePdao storageTableDao,
      AzureAuthService azureAuthService,
      ResourceService resourceService) {
    this.storageTableDao = storageTableDao;
    this.azureAuthService = azureAuthService;
    this.resourceService = resourceService;
  }

  public void loadHistoryToAStorageTable(
      AzureStorageAccountResource storageAccountResource,
      Dataset dataset,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray)
      throws InterruptedException {
    var billingProfile = dataset.getDatasetSummary().getDefaultBillingProfile();
    TableServiceClient serviceClient =
        azureAuthService.getTableServiceClient(
            billingProfile.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());

    storageTableDao.storeLoadHistory(
        serviceClient, dataset.getId(), loadTag, loadTime, loadHistoryArray);
  }

  public List<BulkLoadHistoryModel> getLoadHistory(
      Dataset dataset, String loadTag, int offset, int limit) {
    var billingProfile = dataset.getDatasetSummary().getDefaultBillingProfile();
    var storageAccountResource = resourceService.getDatasetStorageAccount(dataset, billingProfile);
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(
            billingProfile.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());

    return storageTableDao.getLoadHistory(
        tableServiceClient, dataset.getId(), loadTag, offset, limit);
  }

  public void dropLoadHistoryTable(Dataset dataset) {
    var billingProfile = dataset.getDatasetSummary().getDefaultBillingProfile();
    var storageAccountResource = resourceService.getDatasetStorageAccount(dataset, billingProfile);
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(
            billingProfile.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());
    storageTableDao.dropLoadHistoryTable(tableServiceClient, dataset.getId());
  }
}
