package bio.terra.service.tabulardata.azure;

import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
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

    storageTableDao.storeLoadHistory(
        serviceClient, dataset.getId(), loadTag, loadTime, loadHistoryArray);
  }

  public List<BulkLoadHistoryModel> getLoadHistory(
      Dataset dataset, String loadTag, int offset, int limit) {
    var billingProfile = dataset.getDatasetSummary().getDefaultBillingProfile();
    var storageAccountResource =
        resourceService
            .getStorageAccount(dataset, billingProfile)
            .orElseThrow(
                () ->
                    new CorruptMetadataException(
                        String.format(
                            "Expected storage account for Dataset/Billing Profile %s/%s",
                            dataset.getId(), billingProfile.getId())));
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);

    return storageTableDao.getLoadHistory(
        tableServiceClient, dataset.getId(), loadTag, offset, limit);
  }
}
