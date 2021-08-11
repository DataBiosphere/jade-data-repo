package bio.terra.service.tabulardata.azure;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.data.tables.TableServiceClient;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageTableService {

  private final StorageTableDao storageTableDao;
  private final DatasetStorageAccountDao datasetStorageAccountDao;
  private final AzureAuthService azureAuthService;
  private final ResourceService resourceService;

  @Autowired
  public StorageTableService(
      StorageTableDao storageTableDao,
      DatasetStorageAccountDao datasetStorageAccountDao,
      AzureAuthService azureAuthService,
      ResourceService resourceService) {
    this.storageTableDao = storageTableDao;
    this.datasetStorageAccountDao = datasetStorageAccountDao;
    this.azureAuthService = azureAuthService;
    this.resourceService = resourceService;
  }

  public void loadHistoryToAStorageTable(
      Dataset dataset,
      String flightId,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray)
      throws InterruptedException {
    var billingProfile = dataset.getDatasetSummary().getDefaultBillingProfile();
    var storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
    TableServiceClient serviceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);

    storageTableDao.loadHistoryToAStorageTable(
        serviceClient, dataset.getName(), loadTag, loadTime, loadHistoryArray);
  }

  public List<BulkLoadHistoryModel> getLoadHistory(
      Dataset dataset, String loadTag, int offset, int limit) {
    var storageAccountResourceIds =
        datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId());
    var storageAccountResources =
        storageAccountResourceIds.stream()
            .map(resourceService::lookupStorageAccount)
            .collect(Collectors.toList());
    var storageAccountResource = storageAccountResources.get(0);
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(
            dataset.getDatasetSummary().getDefaultBillingProfile(), storageAccountResource);

    return storageTableDao.getLoadHistory(
        tableServiceClient, dataset.getName(), loadTag, offset, limit);
  }
}
