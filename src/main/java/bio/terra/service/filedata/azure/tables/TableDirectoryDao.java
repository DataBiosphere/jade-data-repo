package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.data.tables.models.TableTransactionAction;
import com.azure.data.tables.models.TableTransactionActionType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Paths and document names Azure uses forward slash (/) for its path separator. We also use forward
 * slash in our file system paths. To get uniqueness of files, we want to name files with their full
 * path. Otherwise, two threads could create the same file as two different documents. That would
 * not do at all.
 *
 * <p>We solve this problem by using Azure document names that replace the forward slash in our
 * paths with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C) That replacement is
 * <strong>only</strong> used for the Azure document names. All the other paths we process use the
 * forward slash separator.
 *
 * <p>We need a root directory to hold the other directories. Since we are doing Azure lookup by
 * document name, the root directory needs a name. We call it "_dr_"; it could be anything, but it
 * helps when viewing Azure in the console that it has an obvious name.
 *
 * <p>We don't store the root directory in the paths stored in file and directory entries. It is
 * only used for the Azure lookup. When we refer to paths in the code we talk about: - lookup path -
 * the path used for the Azure lookup. When building this path (and only this path) we prepended it
 * with "_dr_" as the name for the root directory. - directory path - the directory path to the
 * directory containing entry - not including the entry name - full path - the full path to the
 * entry including the entry name.
 *
 * <p>Within the document we store the directory path to the entry and the entry name. That lets us
 * use the indexes to find the entries in a directory.
 *
 * <p>It is an invariant that there are no empty directories. When a directory becomes empty on a
 * delete, it is deleted. When a directory is needed, we create it. That is all done within
 * transactions so there is never a time where the externally visible state violates that invariant.
 */
@Component
public class TableDirectoryDao {
  private final Logger logger = LoggerFactory.getLogger(TableDirectoryDao.class);
  private static final String TABLE_NAME = "dataset";
  private final FileMetadataUtils fileMetadataUtils;

  @Autowired
  public TableDirectoryDao(FileMetadataUtils fileMetadataUtils) {
    this.fileMetadataUtils = fileMetadataUtils;
  }

  public String encodePathAsAzureRowKey(String path) {
    return path.replaceAll("/", " ");
  }

  public String getPartitionKey(String prefix, String path) {
    String parentDir = fileMetadataUtils.getDirectoryPath(path);
    return prefix + encodePathAsAzureRowKey(parentDir);
  }

  // Note that this does not test for duplicates. If invoked on an existing path it will overwrite
  // the entry. Existence checking is handled at upper layers.
  public void createDirectoryEntry(
      TableServiceClient tableServiceClient, FireStoreDirectoryEntry createEntry) {

    tableServiceClient.createTableIfNotExists(TABLE_NAME);
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    String datasetId = createEntry.getDatasetId();

    // Walk up the lookup directory path, finding missing directories we get to an
    // existing one
    // We will create the ROOT_DIR_NAME directory here if it does not exist.
    String lookupDirPath = fileMetadataUtils.makeLookupPath(createEntry.getPath());
    for (String testPath = lookupDirPath;
        !testPath.isEmpty();
        testPath = fileMetadataUtils.getDirectoryPath(testPath)) {

      // !!! In this case we are using a lookup path
      if (lookupByFilePath(tableServiceClient, datasetId, testPath) != null) {
        break;
      }

      FireStoreDirectoryEntry dirToCreate = fileMetadataUtils.makeDirectoryEntry(testPath);
      String partitionKey = getPartitionKey(datasetId, testPath);
      String rowKey = encodePathAsAzureRowKey(testPath);
      TableEntity entity = FireStoreDirectoryEntry.toTableEntity(partitionKey, rowKey, dirToCreate);
      logger.info("Creating directory entry for {} in table {}", testPath, TABLE_NAME);
      tableClient.createEntity(entity);
    }

    String fullPath = fileMetadataUtils.getFullPath(createEntry.getPath(), createEntry.getName());
    String lookupPath = fileMetadataUtils.makeLookupPath(fullPath);
    String partitionKey = getPartitionKey(datasetId, lookupPath);
    String rowKey = encodePathAsAzureRowKey(lookupPath);
    TableEntity createEntryEntity =
        FireStoreDirectoryEntry.toTableEntity(partitionKey, rowKey, createEntry);
    logger.info("Creating directory entry for {} in table {}", fullPath, TABLE_NAME);
    tableClient.createEntity(createEntryEntity);
  }

  // true - directory entry existed and was deleted; false - directory entry did not exist
  public boolean deleteDirectoryEntry(TableServiceClient tableServiceClient, String fileId) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);

    // Look up the directory entry by id. If it doesn't exist, we're done
    TableEntity leafEntity = lookupByFileId(tableServiceClient, fileId);
    if (leafEntity == null) {
      return false;
    }

    List<TableTransactionAction> deleteList = new ArrayList<>();
    TableTransactionAction t =
        new TableTransactionAction(TableTransactionActionType.DELETE, leafEntity);
    deleteList.add(t);

    FireStoreDirectoryEntry leafEntry = FireStoreDirectoryEntry.fromTableEntity(leafEntity);
    String datasetId = leafEntry.getDatasetId();
    String lookupPath = fileMetadataUtils.makeLookupPath(leafEntry.getPath());
    while (!lookupPath.isEmpty()) {
      // Count the number of entries with this path as their directory path
      // A value of 1 means that the directory will be empty after its child is
      // deleted, so we should delete it also.
      ListEntitiesOptions options =
          new ListEntitiesOptions()
              .setFilter(
                  String.format(
                      "path eq '%s'", fileMetadataUtils.makePathFromLookupPath(lookupPath)));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      int size = List.of(entities).size();
      if (size > 1) {
        break;
      }
      TableEntity entity =
          lookupByFilePath(tableServiceClient, datasetId, encodePathAsAzureRowKey(lookupPath));
      deleteList.add(new TableTransactionAction(TableTransactionActionType.DELETE, entity));
      lookupPath = fileMetadataUtils.getDirectoryPath(lookupPath);
    }
    tableClient.submitTransaction(deleteList);
    return true;
  }

  // Each dataset/snapshot has its own set of tables, so we delete the entire directory entry table
  public void deleteDirectoryEntriesFromCollection(TableServiceClient tableServiceClient) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    tableClient.deleteTable();
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveById(
      TableServiceClient tableServiceClient, String fileId) {
    TableEntity entity = lookupByFileId(tableServiceClient, fileId);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveByPath(
      TableServiceClient tableServiceClient, String datasetId, String fullPath) {
    String lookupPath = fileMetadataUtils.makeLookupPath(fullPath);
    TableEntity entity = lookupByFilePath(tableServiceClient, datasetId, lookupPath);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  public List<UUID> validateRefIds(TableServiceClient tableServiceClient, List<UUID> refIdArray) {
    logger.info("validateRefIds for {} file ids", refIdArray.size());
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    List<UUID> missingIds = new ArrayList<>();
    for (UUID s : refIdArray) {
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("fileId eq '%s'", s.toString()));
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      if (!entities.iterator().hasNext()) {
        missingIds.add(s);
      }
    }
    return missingIds;
  }

  List<FireStoreDirectoryEntry> enumerateDirectory(
      TableServiceClient tableServiceClient, String dirPath) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    ListEntitiesOptions options =
        new ListEntitiesOptions().setFilter(String.format("path eq '%s'", dirPath));
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    return entities.stream()
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .collect(Collectors.toList());
  }

  private TableEntity lookupByFilePath(
      TableServiceClient tableServiceClient, String datasetId, String lookupPath) {
    TableClient tableClient = tableServiceClient.getTableClient(TABLE_NAME);
    String partitionKey = getPartitionKey(datasetId, lookupPath);
    String rowKey = encodePathAsAzureRowKey(lookupPath);
    try {
      return tableClient.getEntity(partitionKey, rowKey);
    } catch (TableServiceException ex) {
      return null;
    }
  }

  // Returns null if not found
  private TableEntity lookupByFileId(TableServiceClient tableServiceClient, String fileId) {
    try {
      TableClient client = tableServiceClient.getTableClient(TABLE_NAME);
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("fileId eq '%s'", fileId));
      PagedIterable<TableEntity> entities = client.listEntities(options, null, null);
      if (!entities.iterator().hasNext()) {
        return null;
      }
      int count = 0;
      for (TableEntity entity : entities) {
        count += 1;
        if (count > 1) {
          logger.warn("Found more than one entry for file {}", fileId);
          throw new FileSystemAbortTransactionException("lookupByFileId found too many entries");
        }
      }
      return entities.iterator().next();

    } catch (TableServiceException ex) {
      throw new FileSystemExecutionException("lookupByFileId operation failed");
    }
  }

  // TODO: Implement snapshot-specific methods

}
