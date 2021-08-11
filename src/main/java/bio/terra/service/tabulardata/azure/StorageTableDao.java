package bio.terra.service.tabulardata.azure;

import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StorageTableDao {

  private static final String LOAD_HISTORY_TABLE_NAME_SUFFIX = "LoadHistory";
  private static final int AZURE_STORAGE_TABLE_NAME_MAX_LENGTH = 63;
  private static final int TABLE_NAME_MAX_LENGTH_BEFORE_PREFIX =
      AZURE_STORAGE_TABLE_NAME_MAX_LENGTH - LOAD_HISTORY_TABLE_NAME_SUFFIX.length();

  private final ResourceService resourceService;
  private final AzureAuthService azureAuthService;

  @Autowired
  public StorageTableDao(ResourceService resourceService, AzureAuthService azureAuthService) {
    this.resourceService = resourceService;
    this.azureAuthService = azureAuthService;
  }

  /**
   * Store the results of a bulk file load in an Azure Storage Table
   *
   * <p>The table name will be the result of the dataset name passed through {@link
   * StorageTableDao#azurfyTableName} Entities will be partitioned on the {@param loadTag} and their
   * row keys will be the value of{@link BulkLoadHistoryModel#getFileId()}
   *
   * @param serviceClient A service client for the dataset
   * @param datasetName the name of the dataset
   * @param loadTag Load tag to partition on
   * @param loadTime The time the load occurred
   * @param loadHistoryArray The models to store
   */
  public void loadHistoryToAStorageTable(
      TableServiceClient serviceClient,
      String datasetName,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray) {
    var tableName = azurfyTableName(datasetName);
    TableClient client = serviceClient.createTableIfNotExists(tableName);
    loadHistoryArray.stream()
        .map(model -> bulkFileLoadModelToStorageTableEntity(model, loadTag, loadTime))
        .forEach(client::createEntity);
  }

  /**
   * Get the load history results from storage tables
   *
   * @param tableServiceClient A client for the dataset
   * @param datasetName The dataset name
   * @param loadTag The load tag of the file load
   * @param offset Results will be offset by this much
   * @param limit Results will be limited to this many
   * @return The load history for a load tag within the confines of offset and limit.
   */
  public List<BulkLoadHistoryModel> getLoadHistory(
      TableServiceClient tableServiceClient,
      String datasetName,
      String loadTag,
      int offset,
      int limit) {
    var tableClient = tableServiceClient.getTableClient(azurfyTableName(datasetName));
    ListEntitiesOptions options =
        new ListEntitiesOptions().setFilter(String.format("PartitionKey eq '%s'", loadTag));
    var pagedEntities = tableClient.listEntities(options, null, null);
    // This could be more efficient, but would need to implement some sort of index to query on.
    return pagedEntities.stream()
        .skip(offset)
        .limit(limit)
        .map(StorageTableDao::storageTableEntityBulkFileLoadModel)
        .collect(Collectors.toList());
  }

  private static TableEntity bulkFileLoadModelToStorageTableEntity(
      BulkLoadHistoryModel model, String loadTag, Instant loadTime) {
    return new TableEntity(loadTag, model.getFileId())
        .addProperty("load_tag", loadTag)
        .addProperty("load_time", loadTime)
        .addProperty("source_name", model.getSourcePath())
        .addProperty("target_path", model.getTargetPath())
        .addProperty("state", model.getState().name())
        .addProperty("file_id", model.getFileId())
        .addProperty("checksum_crc32c", model.getChecksumCRC())
        .addProperty("checksum_md5", model.getChecksumMD5())
        .addProperty("error", model.getError());
  }

  private static BulkLoadHistoryModel storageTableEntityBulkFileLoadModel(TableEntity tableEntity) {
    var model =
        new BulkLoadHistoryModel()
            .sourcePath(tableEntity.getProperty("source_name").toString())
            .targetPath(tableEntity.getProperty("target_path").toString())
            .state(BulkLoadFileState.valueOf(tableEntity.getProperty("state").toString()))
            .fileId(tableEntity.getProperty("file_id").toString());

    Optional.ofNullable(tableEntity.getProperty("checksum_crc32c"))
        .ifPresent(o -> model.checksumCRC(o.toString()));
    Optional.ofNullable(tableEntity.getProperty("checksum_md5"))
        .ifPresent(o -> model.checksumMD5(o.toString()));
    Optional.ofNullable(tableEntity.getProperty("error")).ifPresent(o -> model.error(o.toString()));
    return model;
  }

  /**
   * Generate a valid azure storage table name for load history. The resulting name will be the
   * input truncated to fit {@value #LOAD_HISTORY_TABLE_NAME_SUFFIX} as a suffix in less than 63
   * characters, with all non-alphanumeric characters stripped out.
   *
   * @param tableName The root of the table name
   * @return A valid azure storage table name with load history suffix.
   */
  private static String azurfyTableName(String tableName) {
    var alphaNumeric = tableName.replaceAll("[^A-Za-z0-9]", "");
    final String rightLength;
    if (alphaNumeric.length() <= TABLE_NAME_MAX_LENGTH_BEFORE_PREFIX) {
      rightLength = alphaNumeric + LOAD_HISTORY_TABLE_NAME_SUFFIX;
    } else {
      rightLength =
          alphaNumeric.substring(0, TABLE_NAME_MAX_LENGTH_BEFORE_PREFIX)
              + LOAD_HISTORY_TABLE_NAME_SUFFIX;
    }
    if (rightLength.substring(0, 1).matches("[0-9]")) {
      return "a" + rightLength.substring(1);
    } else {
      return rightLength;
    }
  }
}
