package bio.terra.service.filedata.azure.tables;

import bio.terra.common.FutureUtils;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
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
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
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
  private static final int MAX_FILTER_CLAUSES = 15;
  private final ConfigurationService configurationService;
  private final AsyncTaskExecutor azureTableThreadpool;

  public TableDirectoryDao(
      ConfigurationService configurationService,
      @Qualifier("azureTableThreadpool") AsyncTaskExecutor azureTableThreadpool) {
    this.configurationService = configurationService;
    this.azureTableThreadpool = azureTableThreadpool;
  }

  public String encodePathAsAzureRowKey(String path) {
    return path.replaceAll("/", " ");
  }

  public String getCollectionPartitionKey(UUID collectionId, String path) {
    return getPartitionKey(collectionId.toString(), path);
  }

  public String getPartitionKey(String prefix, String path) {
    String parentDir = FileMetadataUtils.getDirectoryPath(path);
    return prefix + encodePathAsAzureRowKey(parentDir);
  }

  // Note that this does not test for duplicates. If invoked on an existing path it will overwrite
  // the entry. Existence checking is handled at upper layers.
  public void createDirectoryEntry(
      TableServiceClient tableServiceClient,
      UUID collectionId,
      String tableName,
      FireStoreDirectoryEntry createEntry) {

    tableServiceClient.createTableIfNotExists(tableName);
    TableClient tableClient = tableServiceClient.getTableClient(tableName);

    // Walk up the lookup directory path, finding missing directories we get to an
    // existing one
    // We will create the ROOT_DIR_NAME directory here if it does not exist.
    String lookupDirPath = FileMetadataUtils.makeLookupPath(createEntry.getPath());
    for (String testPath = lookupDirPath;
        !testPath.isEmpty();
        testPath = FileMetadataUtils.getDirectoryPath(testPath)) {

      // !!! In this case we are using a lookup path
      if (lookupByFilePath(tableServiceClient, collectionId, tableName, testPath) != null) {
        break;
      }

      FireStoreDirectoryEntry dirToCreate = FileMetadataUtils.makeDirectoryEntry(testPath);
      String partitionKey = getCollectionPartitionKey(collectionId, testPath);
      String rowKey = encodePathAsAzureRowKey(testPath);
      TableEntity entity = FireStoreDirectoryEntry.toTableEntity(partitionKey, rowKey, dirToCreate);
      logger.info("Upserting directory entry for {} in table {}", testPath, tableName);
      // For file ingest worker flights,
      // It's possible that another thread is trying to write the same directory entity at the same
      // time upsert rather than create so that it does not fail if it already exists
      tableClient.upsertEntity(entity);
    }
    createEntityForPath(tableClient, collectionId, tableName, createEntry);
  }

  private void createEntityForPath(
      TableClient tableClient,
      UUID collectionId,
      String tableName,
      FireStoreDirectoryEntry createEntry) {
    String fullPath = FileMetadataUtils.getFullPath(createEntry.getPath(), createEntry.getName());
    String lookupPath = FileMetadataUtils.makeLookupPath(fullPath);
    String partitionKey = getCollectionPartitionKey(collectionId, lookupPath);
    String rowKey = encodePathAsAzureRowKey(lookupPath);
    TableEntity createEntryEntity =
        FireStoreDirectoryEntry.toTableEntity(partitionKey, rowKey, createEntry);
    logger.info("Upserting directory entry for {} in table {}", fullPath, tableName);
    tableClient.upsertEntity(createEntryEntity);
  }

  // true - directory entry existed and was deleted; false - directory entry did not exist
  public boolean deleteDirectoryEntry(
      TableServiceClient tableServiceClient, UUID collectionId, String tableName, String fileId) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);

    // Look up the directory entry by id. If it doesn't exist, we're done
    TableEntity leafEntity = lookupByFileId(tableServiceClient, tableName, fileId);
    if (leafEntity == null) {
      return false;
    }
    tableClient.deleteEntity(leafEntity);

    FireStoreDirectoryEntry leafEntry = FireStoreDirectoryEntry.fromTableEntity(leafEntity);
    String lookupPath = FileMetadataUtils.makeLookupPath(leafEntry.getPath());
    while (!lookupPath.isEmpty()) {
      // If there are no entries that share this path as their directory path,
      // delete the directory entry
      String filterPath = FileMetadataUtils.makePathFromLookupPath(lookupPath);
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("path eq '%s'", filterPath));
      if (TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
        break;
      }
      TableEntity entity =
          lookupByFilePath(tableServiceClient, collectionId, tableName, lookupPath);
      if (entity != null) {
        tableClient.deleteEntity(entity);
      }
      lookupPath = FileMetadataUtils.getDirectoryPath(lookupPath);
    }
    return true;
  }

  // Each dataset/snapshot has its own set of tables, so we delete the entire directory entry table
  public void deleteDirectoryEntriesFromCollection(
      TableServiceClient tableServiceClient, String tableName) {
    if (TableServiceClientUtils.tableExists(tableServiceClient, tableName)) {
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      tableClient.deleteTable();
    } else {
      logger.warn(
          "No storage table {} found to be removed.  This should only happen when deleting a file-less dataset.",
          tableName);
    }
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveById(
      TableServiceClient tableServiceClient, String tableName, String fileId) {
    TableEntity entity = lookupByFileId(tableServiceClient, tableName, fileId);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveByPath(
      TableServiceClient tableServiceClient, UUID collectionId, String tableName, String fullPath) {
    String lookupPath = FileMetadataUtils.makeLookupPath(fullPath);
    TableEntity entity = lookupByFilePath(tableServiceClient, collectionId, tableName, lookupPath);
    return Optional.ofNullable(entity)
        .map(d -> FireStoreDirectoryEntry.fromTableEntity(entity))
        .orElse(null);
  }

  public List<FireStoreDirectoryEntry> batchRetrieveByPath(
      TableServiceClient tableServiceClient,
      UUID collectionId,
      String tableName,
      Collection<String> fullPaths) {
    return fullPaths.stream()
        .map(FileMetadataUtils::makeLookupPath)
        // Possible to pass in path both with and without prefix, so want to check for uniqueness
        .distinct()
        .map(path -> lookupByFilePath(tableServiceClient, collectionId, tableName, path))
        .filter(Objects::nonNull)
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .collect(Collectors.toList());
  }

  public List<String> validateRefIds(
      TableServiceClient tableServiceClient, UUID datasetId, List<String> refIdArray) {
    logger.info("validateRefIds for {} file ids", refIdArray.size());
    List<Future<Stream<String>>> futures = new ArrayList<>();
    for (List<String> refIdChunk : ListUtils.partition(refIdArray, MAX_FILTER_CLAUSES)) {
      futures.add(
          azureTableThreadpool.submit(
              () -> {
                List<TableEntity> fileRefs =
                    TableServiceClientUtils.batchRetrieveFiles(
                        tableServiceClient, datasetId, refIdChunk);
                // if no files were retrieved, then every file in list is not valid
                if (fileRefs.isEmpty()) {
                  return refIdChunk.stream();
                }
                // if any files were retrieved, then remove from invalid list
                Set<String> validRefIds =
                    fileRefs.stream()
                        .map(e -> e.getProperty("fileId").toString())
                        .collect(Collectors.toSet());
                return refIdChunk.stream().filter(id -> !validRefIds.contains(id));
              }));
    }
    // Flat map the results of the futures since each future returns a list of refIds
    return FutureUtils.waitFor(futures).stream().flatMap(s -> s).toList();
  }

  public List<FireStoreDirectoryEntry> enumerateAll(
      TableServiceClient tableServiceClient, String tableName) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    ListEntitiesOptions options = new ListEntitiesOptions();
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    return entities.stream()
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .collect(Collectors.toList());
  }

  /** results are sorted by partition key and row key * */
  public List<FireStoreDirectoryEntry> enumerateFileRefEntries(
      TableServiceClient tableServiceClient, String collectionId, int offset, int limit) {
    TableClient tableClient = tableServiceClient.getTableClient(collectionId);
    ListEntitiesOptions options = new ListEntitiesOptions().setFilter("isFileRef eq true");
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    if (!entities.iterator().hasNext()) {
      return List.of();
    }
    return entities.stream()
        .skip(offset)
        .limit(limit)
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .toList();
  }

  List<FireStoreDirectoryEntry> enumerateDirectory(
      TableServiceClient tableServiceClient, String tableName, String dirPath) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    ListEntitiesOptions options =
        new ListEntitiesOptions().setFilter(String.format("path eq '%s'", dirPath));
    // TODO - add check that there are entries
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    return entities.stream()
        .map(FireStoreDirectoryEntry::fromTableEntity)
        .collect(Collectors.toList());
  }

  TableEntity lookupByFilePath(
      TableServiceClient tableServiceClient,
      UUID collectionId,
      String tableName,
      String lookupPath) {
    TableClient tableClient = tableServiceClient.getTableClient(tableName);
    String partitionKey = getCollectionPartitionKey(collectionId, lookupPath);
    String rowKey = encodePathAsAzureRowKey(lookupPath);
    try {
      return tableClient.getEntity(partitionKey, rowKey);
    } catch (TableServiceException ex) {
      return null;
    }
  }

  // Returns null if not found
  private TableEntity lookupByFileId(
      TableServiceClient tableServiceClient, String tableName, String fileId) {
    try {
      ListEntitiesOptions options =
          new ListEntitiesOptions().setFilter(String.format("fileId eq '%s'", fileId));
      if (!TableServiceClientUtils.tableHasEntries(tableServiceClient, tableName, options)) {
        return null;
      } else if (!TableServiceClientUtils.tableHasSingleEntry(
          tableServiceClient, tableName, options)) {
        throw new FileSystemAbortTransactionException(
            String.format("lookupByFileId found too many entries for fileId %s", fileId));
      }
      TableClient tableClient = tableServiceClient.getTableClient(tableName);
      PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
      return entities.iterator().next();

    } catch (TableServiceException ex) {
      throw new FileSystemExecutionException("lookupByFileId operation failed");
    }
  }

  // -- Snapshot filesystem methods --

  // To improve performance of building the snapshot file system, we use three techniques:
  // 1. Operate over batches so that we can issue requests to fire store in parallel
  // 2. Cache directory paths so that we do not due extra lookups or creates on shared directory
  // structure
  // 3. Rewrite rather than read, check existence, and then write. The logic here is that there is
  // no contention so the writing doesn't generate conflicts, and the typical use case is that
  // overwrites will be rare:
  //      a. File references are usually unique in the datasets we know about
  //      b. Directories are cached, so will be overwritten based on the effectiveness of the cache

  public void addEntriesToSnapshot(
      TableServiceClient datasetTableServiceClient,
      TableServiceClient snapshotTableServiceClient,
      UUID datasetId,
      String datasetDirName,
      UUID snapshotId,
      Set<String> fileIds,
      boolean usesGlobalFileIds) {
    int cacheSize = configurationService.getParameterValue(ConfigEnum.SNAPSHOT_CACHE_SIZE);
    LRUMap<String, Boolean> pathMap = new LRUMap<>(cacheSize);
    storeTopDirectory(snapshotTableServiceClient, snapshotId, datasetDirName);
    List<Future<Void>> futures = new ArrayList<>();
    for (List<String> fileIdsBatch :
        ListUtils.partition(List.copyOf(fileIds), MAX_FILTER_CLAUSES)) {
      futures.add(
          azureTableThreadpool.submit(
              () -> {
                List<TableEntity> entities =
                    TableServiceClientUtils.batchRetrieveFiles(
                        datasetTableServiceClient, datasetId, fileIdsBatch);

                List<FireStoreDirectoryEntry> directoryEntries =
                    entities.stream()
                        .map(
                            entity -> {
                              FireStoreDirectoryEntry directoryEntry =
                                  FireStoreDirectoryEntry.fromTableEntity(entity);
                              if (!directoryEntry.getIsFileRef()) {
                                throw new FileSystemExecutionException(
                                    "Directories are not supported as references");
                              }
                              return directoryEntry;
                            })
                        .collect(Collectors.toList());
                if (directoryEntries.isEmpty()) {
                  throw new FileSystemExecutionException("No fileIds found in batch lookup");
                }

                // Find directory paths that need to be created; plus add to the cache
                Set<String> newPaths =
                    FileMetadataUtils.findNewDirectoryPaths(directoryEntries, pathMap);
                List<FireStoreDirectoryEntry> datasetDirectoryEntries =
                    batchRetrieveByPath(
                        datasetTableServiceClient,
                        datasetId,
                        StorageTableName.DATASET.toTableName(datasetId),
                        newPaths);

                // Create snapshot file system entries
                List<FireStoreDirectoryEntry> snapshotEntries = new ArrayList<>();
                if (usesGlobalFileIds) {
                  snapshotEntries.addAll(directoryEntries);
                  snapshotEntries.addAll(datasetDirectoryEntries);
                } else {
                  for (FireStoreDirectoryEntry datasetEntry : directoryEntries) {
                    snapshotEntries.add(datasetEntry.copyEntryUnderNewPath(datasetDirName));
                  }
                  for (FireStoreDirectoryEntry datasetEntry : datasetDirectoryEntries) {
                    snapshotEntries.add(datasetEntry.copyEntryUnderNewPath(datasetDirName));
                  }
                }
                // Store the batch of entries. This will override existing entries,
                // but that is not the typical case and it is lower cost just overwrite
                // rather than retrieve to avoid the write.
                batchStoreDirectoryEntry(snapshotTableServiceClient, snapshotId, snapshotEntries);

                return null;
              }));
    }
    FutureUtils.waitFor(futures);
  }

  /**
   * Method used during snapshot create to add the first directory entry We have to create the top
   * directory structure for the dataset and the root folder. Those components cannot be copied from
   * the dataset, but have to be created new in the snapshot directory. We probe to see if the
   * dirName directory exists. If not, we use the createFileRef path to construct it and the parent,
   * if necessary.
   *
   * @param tableServiceClient
   * @param snapshotId
   * @param dirName Dataset name, used as top directory name
   */
  @VisibleForTesting
  void storeTopDirectory(TableServiceClient tableServiceClient, UUID snapshotId, String dirName) {
    String dirPath = "/" + dirName;
    String snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);

    // Check if top directory already exists
    TableEntity directoryEntry =
        lookupByFilePath(tableServiceClient, snapshotId, snapshotTableName, dirPath);
    if (directoryEntry != null) {
      return;
    }

    // Top directory does not exist, so create it
    FireStoreDirectoryEntry topDir =
        new FireStoreDirectoryEntry()
            .fileId(UUID.randomUUID().toString())
            .isFileRef(false)
            .path("/")
            .name(dirName)
            .fileCreatedDate(Instant.now().toString());

    createDirectoryEntry(tableServiceClient, snapshotId, snapshotTableName, topDir);
  }

  void batchStoreDirectoryEntry(
      TableServiceClient snapshotTableServiceClient,
      UUID snapshotId,
      List<FireStoreDirectoryEntry> snapshotEntries) {
    String tableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    TableClient tableClient = snapshotTableServiceClient.getTableClient(tableName);
    FutureUtils.waitFor(
        snapshotEntries.stream()
            .map(
                snapshotEntry ->
                    azureTableThreadpool.submit(
                        () ->
                            createEntityForPath(tableClient, snapshotId, tableName, snapshotEntry)))
            .toList());
  }
}
