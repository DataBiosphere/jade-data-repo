package bio.terra.service.tabulardata.azure;

import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.tabulardata.LoadHistoryUtil;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AzureStorageTablePdao {
  private static final Logger logger = LoggerFactory.getLogger(AzureStorageTablePdao.class);

  private static String computeInternalLoadTag(String loadTag) {
    return Base64.getEncoder().encodeToString(loadTag.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Store the results of a bulk file load in an Azure Storage Table
   *
   * <p>The table name will be the result of the dataset id passed through
   * {@Link:StorageTableName.toTableName(resourceId)} Entities will be partitioned on the loadTag
   * and their row keys will be the value of {@link BulkLoadHistoryModel#getFileId()}
   *
   * @param serviceClient A service client for the dataset
   * @param datasetId the id of the dataset
   * @param loadTag Load tag to partition on
   * @param loadTime The time the load occurred
   * @param loadHistoryArray The models to store
   */
  public void storeLoadHistory(
      TableServiceClient serviceClient,
      UUID datasetId,
      String loadTag,
      Instant loadTime,
      List<BulkLoadHistoryModel> loadHistoryArray) {
    if (loadHistoryArray.isEmpty()) {
      return;
    }
    var tableName = StorageTableName.LOAD_HISTORY.toTableName(datasetId);
    TableClient client = serviceClient.createTableIfNotExists(tableName);
    // if the table already exists, the returned client is null and we have to get it explicitly
    if (client == null) {
      client = serviceClient.getTableClient(tableName);
    }

    var internalLoadTag = computeInternalLoadTag(loadTag);

    ListEntitiesOptions options =
        new ListEntitiesOptions()
            .setFilter(
                String.format(
                    "PartitionKey eq '%s' and %s eq true",
                    internalLoadTag, LoadHistoryUtil.IS_LAST_FIELD_NAME));

    List<TableEntity> lastEntityList =
        client.listEntities(options, null, null).stream().collect(Collectors.toList());
    if (lastEntityList.size() > 1) {
      throw new CorruptMetadataException(
          "There should only be 0 or 1 'last' loaded entity in load history table storage");
    }
    int indexToStartFrom = 0;
    if (lastEntityList.size() == 1) {
      TableEntity lastTableEntity = lastEntityList.get(0);
      StorageTableLoadHistoryEntity lastEntity = new StorageTableLoadHistoryEntity(lastTableEntity);
      lastTableEntity.addProperty(LoadHistoryUtil.IS_LAST_FIELD_NAME, false);
      client.updateEntity(lastTableEntity);
      indexToStartFrom = lastEntity.index + 1;
    }

    for (int i = 0; i < loadHistoryArray.size(); i++) {
      var isLast = i == loadHistoryArray.size() - 1;
      var thisIndex = i + indexToStartFrom;
      BulkLoadHistoryModel historyEntry = loadHistoryArray.get(i);
      client.createEntity(
          bulkFileLoadModelToStorageTableEntity(
              new StorageTableLoadHistoryEntity(historyEntry, internalLoadTag, thisIndex, isLast),
              loadTag,
              loadTime));
    }
  }

  /**
   * Get the load history results from storage tables
   *
   * @param tableServiceClient A client for the dataset
   * @param datasetId The dataset id
   * @param loadTag The load tag of the file load
   * @param offset Results will be offset by this much
   * @param limit Results will be limited to this many
   * @return The load history for a load tag within the confines of offset and limit.
   */
  public List<BulkLoadHistoryModel> getLoadHistory(
      TableServiceClient tableServiceClient,
      UUID datasetId,
      String loadTag,
      int offset,
      int limit) {
    var tableClient =
        tableServiceClient.getTableClient(StorageTableName.LOAD_HISTORY.toTableName(datasetId));
    var internalLoadTag = computeInternalLoadTag(loadTag);
    ListEntitiesOptions options =
        new ListEntitiesOptions()
            .setTop(limit)
            .setFilter(
                String.format(
                    "PartitionKey eq '%s' and %s ge %d and %s lt %s",
                    internalLoadTag,
                    LoadHistoryUtil.INDEX_FIELD_NAME,
                    offset,
                    LoadHistoryUtil.INDEX_FIELD_NAME,
                    offset + limit));
    var pagedEntities = tableClient.listEntities(options, null, null);
    return pagedEntities.stream()
        .map(StorageTableLoadHistoryEntity::new)
        .map(e -> e.model)
        .collect(Collectors.toList());
  }

  public void dropLoadHistoryTable(TableServiceClient tableServiceClient, UUID datasetId) {
    tableServiceClient
        .getTableClient(StorageTableName.LOAD_HISTORY.toTableName(datasetId))
        .deleteTable();
  }

  private static TableEntity bulkFileLoadModelToStorageTableEntity(
      StorageTableLoadHistoryEntity entity, String loadTag, Instant loadTime) {
    var model = entity.model;
    return new TableEntity(entity.partitionKey, UUID.randomUUID().toString())
        .addProperty(LoadHistoryUtil.LOAD_TAG_FIELD_NAME, loadTag)
        .addProperty(LoadHistoryUtil.LOAD_TIME_FIELD_NAME, loadTime)
        .addProperty(LoadHistoryUtil.SOURCE_NAME_FIELD_NAME, model.getSourcePath())
        .addProperty(LoadHistoryUtil.TARGET_PATH_FIELD_NAME, model.getTargetPath())
        .addProperty(LoadHistoryUtil.STATE_FIELD_NAME, model.getState().name())
        .addProperty(LoadHistoryUtil.FILE_ID_FIELD_NAME, model.getFileId())
        .addProperty(LoadHistoryUtil.CHECKSUM_CRC32C_FIELD_NAME, model.getChecksumCRC())
        .addProperty(LoadHistoryUtil.CHECKSUM_MD5_FIELD_NAME, model.getChecksumMD5())
        .addProperty(LoadHistoryUtil.ERROR_FIELD_NAME, model.getError())
        .addProperty(LoadHistoryUtil.INDEX_FIELD_NAME, entity.index)
        .addProperty(LoadHistoryUtil.IS_LAST_FIELD_NAME, entity.isLast);
  }

  private static class StorageTableLoadHistoryEntity {

    final BulkLoadHistoryModel model;
    final String partitionKey;
    final int index;
    final boolean isLast;

    public StorageTableLoadHistoryEntity(
        BulkLoadHistoryModel model, String partitionKey, int index, boolean isLast) {
      this.model = model;
      this.partitionKey = partitionKey;
      this.index = index;
      this.isLast = isLast;
    }

    public StorageTableLoadHistoryEntity(TableEntity tableEntity) {
      this.model = storageTableEntityBulkFileLoadModel(tableEntity);
      this.partitionKey = tableEntity.getPartitionKey();
      this.index =
          Integer.parseInt(tableEntity.getProperty(LoadHistoryUtil.INDEX_FIELD_NAME).toString());
      this.isLast =
          Boolean.parseBoolean(
              tableEntity.getProperty(LoadHistoryUtil.IS_LAST_FIELD_NAME).toString());
    }

    private BulkLoadHistoryModel storageTableEntityBulkFileLoadModel(TableEntity tableEntity) {
      return new BulkLoadHistoryModel()
          .sourcePath(tableEntity.getProperty(LoadHistoryUtil.SOURCE_NAME_FIELD_NAME).toString())
          .targetPath(tableEntity.getProperty(LoadHistoryUtil.TARGET_PATH_FIELD_NAME).toString())
          .state(
              BulkLoadFileState.valueOf(
                  tableEntity.getProperty(LoadHistoryUtil.STATE_FIELD_NAME).toString()))
          .fileId((String) tableEntity.getProperty(LoadHistoryUtil.FILE_ID_FIELD_NAME))
          .checksumCRC((String) tableEntity.getProperty(LoadHistoryUtil.CHECKSUM_CRC32C_FIELD_NAME))
          .checksumMD5((String) tableEntity.getProperty(LoadHistoryUtil.CHECKSUM_MD5_FIELD_NAME))
          .error((String) tableEntity.getProperty(LoadHistoryUtil.ERROR_FIELD_NAME));
    }
  }
}
