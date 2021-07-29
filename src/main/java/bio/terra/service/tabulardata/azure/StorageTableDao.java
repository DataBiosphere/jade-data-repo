package bio.terra.service.tabulardata.azure;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.TableEntity;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageTableDao {

  private final String PARTITION_KEY = "partitionKey";
  private final String LOAD_HISTORY_TABLE_NAME_SUFFIX = "_loadHistory";

  private final ResourceService resourceService;
  private final AzureAuthService azureAuthService;

  @Autowired
  public StorageTableDao(ResourceService resourceService, AzureAuthService azureAuthService) {
    this.resourceService = resourceService;
    this.azureAuthService = azureAuthService;
  }

  public void loadHistoryToAStorageTable(
      Dataset dataset,
      BillingProfileModel billingProfileModel,
      String flightId,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray)
      throws InterruptedException {
    var storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfileModel, flightId);
    TableServiceClient serviceClient =
        azureAuthService.getTableServiceClient(billingProfileModel, storageAccountResource);
    var tableName = dataset.getName() + LOAD_HISTORY_TABLE_NAME_SUFFIX;
    TableClient client = serviceClient.createTableIfNotExists(tableName);

    loadHistoryArray.stream()
        .map(model -> bulkFileLoadModelToStorageTableEntity(model, loadTag, loadTime))
        .forEach(client::createEntity);
  }

  private TableEntity bulkFileLoadModelToStorageTableEntity(
      BulkLoadHistoryModel model, String loadTag, Instant loadTime) {
    return new TableEntity(PARTITION_KEY, model.getFileId())
        .addProperty("load_tag", loadTag)
        .addProperty("load_time", loadTime)
        .addProperty("source_name", model.getSourcePath())
        .addProperty("target_path", model.getTargetPath())
        .addProperty("state", model.getState())
        .addProperty("file_id", model.getFileId())
        .addProperty("checksum_crc32c", model.getChecksumCRC())
        .addProperty("checksum_md5", model.getChecksumMD5())
        .addProperty("error", model.getError());
  }
}
