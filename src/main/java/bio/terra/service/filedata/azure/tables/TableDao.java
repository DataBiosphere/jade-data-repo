package bio.terra.service.filedata.azure.tables;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.CollectionType;
import bio.terra.model.CloudPlatform;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSDir;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.SnapshotCompute;
import bio.terra.service.filedata.SnapshotComputeHelper;
import bio.terra.service.filedata.exception.FileNotFoundException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.firestore.InterruptibleConsumer;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import com.azure.data.tables.TableServiceClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// Operations on a file often need to touch file and directory collections that is,
// the Azure TableFileDao and the TableDirectoryDao.
// The data to make an FSDir or FSFile is now spread between the file collection and the
// directory collection, so a lookup needs to visit two places to generate a complete FSItem.
// This class coordinates operations between the daos.
//
// The dependency collection is independent, so it is not included under this dao.
// Perhaps it should be.
//
// There are several functions performed in this layer.
//  1. Encapsulating the underlying daos
//  2. Converting from dao objects into DR metadata objects
//  3. Dealing with project, dataset, and snapshot objects, so the daos don't have to
//
@Component
public class TableDao {
  private final Logger logger = LoggerFactory.getLogger(TableDao.class);

  private final TableDirectoryDao directoryDao;
  private final TableFileDao fileDao;
  private final AzureAuthService azureAuthService;
  private final ConfigurationService configurationService;
  private final PerformanceLogger performanceLogger;

  @Autowired
  public TableDao(
      TableDirectoryDao directoryDao,
      TableFileDao fileDao,
      AzureAuthService azureAuthService,
      ConfigurationService configurationService,
      PerformanceLogger performanceLogger) {
    this.directoryDao = directoryDao;
    this.fileDao = fileDao;
    this.azureAuthService = azureAuthService;
    this.configurationService = configurationService;
    this.performanceLogger = performanceLogger;
  }

  public void createDirectoryEntry(
      FireStoreDirectoryEntry newEntry,
      AzureStorageAuthInfo storageAuthInfo,
      UUID collectionId,
      String tableName) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    directoryDao.createDirectoryEntry(tableServiceClient, collectionId, tableName, newEntry);
  }

  public boolean deleteDirectoryEntry(
      String fileId, AzureStorageAuthInfo storageAuthInfo, UUID collectionId, String tableName) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    return directoryDao.deleteDirectoryEntry(tableServiceClient, collectionId, tableName, fileId);
  }

  public void createFileMetadata(
      String collectionId, FireStoreFile newFile, AzureStorageAuthInfo storageAuthInfo) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    fileDao.createFileMetadata(tableServiceClient, collectionId, newFile);
  }

  public boolean deleteFileMetadata(
      String collectionId, String fileId, AzureStorageAuthInfo storageAuthInfo) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    return fileDao.deleteFileMetadata(tableServiceClient, collectionId, fileId);
  }

  public void deleteFilesFromDataset(
      AzureStorageAuthInfo storageAuthInfo,
      UUID datasetId,
      InterruptibleConsumer<FireStoreFile> func) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    if (configurationService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
      // If we didn't load files, don't try to delete them
      fileDao.deleteFilesFromDataset(tableServiceClient, datasetId, f -> {});
    } else {
      logger.info("deleting files from dataset");
      fileDao.deleteFilesFromDataset(tableServiceClient, datasetId, func);
    }
    logger.info("deleting directory entries");
    directoryDao.deleteDirectoryEntriesFromCollection(
        tableServiceClient, StorageTableName.DATASET.toTableName(datasetId));
  }

  public void deleteFilesFromSnapshot(AzureStorageAuthInfo storageAuthInfo, UUID snapshotId) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    logger.info("deleting directory entries");
    directoryDao.deleteDirectoryEntriesFromCollection(
        tableServiceClient, StorageTableName.SNAPSHOT.toTableName(snapshotId));
  }

  public FireStoreDirectoryEntry lookupDirectoryEntryByPath(
      Dataset dataset, String path, AzureStorageAuthInfo storageAuthInfo) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    return directoryDao.retrieveByPath(
        tableServiceClient,
        dataset.getId(),
        StorageTableName.DATASET.toTableName(dataset.getId()),
        path);
  }

  public FireStoreFile lookupFile(
      String collectionId, String fileId, AzureStorageAuthInfo storageAuthInfo) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    return fileDao.retrieveFileMetadata(tableServiceClient, collectionId, fileId);
  }

  public List<FSFile> batchRetrieveFiles(
      String collectionId,
      AzureStorageAuthInfo storageAuthInfo,
      String datasetId,
      AzureStorageAuthInfo datasetStorageAuthInfo,
      int offset,
      int limit) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    List<FireStoreDirectoryEntry> directoryEntries =
        directoryDao.enumerateFileRefEntries(tableServiceClient, collectionId, offset, limit);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);
    List<FireStoreFile> files =
        fileDao.batchRetrieveFileMetadata(datasetTableServiceClient, datasetId, directoryEntries);
    return FileMetadataUtils.toFSFiles(directoryEntries, files);
  }

  public void snapshotCompute(
      Snapshot snapshot,
      TableServiceClient snapshotTableServiceClient,
      TableServiceClient datasetTableServiceClient)
      throws InterruptedException {

    UUID datasetId = snapshot.getSourceDataset().getId();
    UUID snapshotId = snapshot.getId();
    String snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    FireStoreDirectoryEntry topDir =
        directoryDao.retrieveByPath(snapshotTableServiceClient, snapshotId, snapshotTableName, "/");
    // If topDir is null, it means no files were added to the snapshot file system in the previous
    // step. So there is nothing to compute
    if (topDir != null) {
      // We batch the updates to firestore by collecting updated entries into this list,
      // and when we get enough, writing them out.
      List<FireStoreDirectoryEntry> updateBatch = new ArrayList<>();

      String retrieveTimer = performanceLogger.timerStart();

      StorageTableComputeHelper helper =
          getHelper(datasetTableServiceClient, snapshotTableServiceClient, datasetId, snapshotId);
      SnapshotCompute.computeDirectory(helper, topDir, updateBatch);

      performanceLogger.timerEndAndLog(
          retrieveTimer,
          snapshotId.toString(), // not a flight, so no job id
          this.getClass().getName(),
          "tableDao.computeDirectoryGetMetadata");

      // Write the last batch out
      directoryDao.batchStoreDirectoryEntry(snapshotTableServiceClient, snapshotId, updateBatch);
    }
  }

  /**
   * Retrieve an FSItem by path
   *
   * @param datasetId - dataset containing file's directory entry
   * @param fullPath - path of the file in the directory
   * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all; 1
   *     means contents of this directory; 2 means this and its directories, etc. -1 means the
   *     entire tree.
   * @return FSFile or FSDir of retrieved file; can return null on not found //
   */
  // TODO - Azure snapshot: Support passing in snapshotID
  public FSItem retrieveByPath(
      UUID datasetId, String fullPath, int enumerateDepth, AzureStorageAuthInfo storageAuthInfo) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    FireStoreDirectoryEntry fireStoreDirectoryEntry =
        directoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            fullPath);
    return retrieveWorker(
        tableServiceClient,
        tableServiceClient,
        datasetId.toString(),
        enumerateDepth,
        fireStoreDirectoryEntry,
        fullPath);
  }

  public Optional<FSItem> lookupOptionalPath(
      UUID datasetId, String fullPath, AzureStorageAuthInfo storageAuthInfo, int enumerateDepth) {
    TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    FireStoreDirectoryEntry fireStoreDirectoryEntry =
        directoryDao.retrieveByPath(
            tableServiceClient,
            datasetId,
            StorageTableName.DATASET.toTableName(datasetId),
            fullPath);
    return Optional.ofNullable(fireStoreDirectoryEntry)
        .map(
            entry ->
                retrieveWorker(
                    tableServiceClient,
                    tableServiceClient,
                    datasetId.toString(),
                    enumerateDepth,
                    entry,
                    fullPath));
  }

  /**
   * Retrieve an FSItem by id
   *
   * @param collectionType - The type of collection this represents
   * @param datasetId - ID for dataset containing file's metadata entry
   * @param collectionId - ID for dataset or snapshot containing file's directory entry
   * @param fileId - id of the file or directory
   * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all; 1
   *     means contents of this directory; 2 means this and its directories, etc. -1 means the
   *     entire tree.
   * @param tableStorageAuthInfo - an AzureStorageAuthInfo object for connecting to the table
   *     service client
   * @return FSFile or FSDir of retrieved file; can return null on not found
   */
  public FSItem retrieveById(
      CollectionType collectionType,
      UUID datasetId,
      UUID collectionId,
      String fileId,
      int enumerateDepth,
      AzureStorageAuthInfo tableStorageAuthInfo,
      AzureStorageAuthInfo datasetTableStorageAuthInfo) {
    TableServiceClient tableServiceClient =
        azureAuthService.getTableServiceClient(tableStorageAuthInfo);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetTableStorageAuthInfo);
    FireStoreDirectoryEntry fireStoreDirectoryEntry;
    switch (collectionType) {
      case DATASET:
        fireStoreDirectoryEntry =
            directoryDao.retrieveById(
                tableServiceClient, StorageTableName.DATASET.toTableName(collectionId), fileId);
        break;
      case SNAPSHOT:
        fireStoreDirectoryEntry =
            directoryDao.retrieveById(
                tableServiceClient, StorageTableName.SNAPSHOT.toTableName(collectionId), fileId);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid collection type: %s", collectionType));
    }
    return retrieveWorker(
        tableServiceClient,
        datasetTableServiceClient,
        datasetId.toString(),
        enumerateDepth,
        fireStoreDirectoryEntry,
        fileId);
  }

  public List<String> retrieveAllFileIds(TableServiceClient tableServiceClient, UUID snapshotId) {
    return directoryDao
        .enumerateAll(tableServiceClient, StorageTableName.SNAPSHOT.toTableName(snapshotId))
        .stream()
        .map(FireStoreDirectoryEntry::getFileId)
        .toList();
  }

  //   -- private methods --

  /**
   * Retrieves an FSItem object
   *
   * @param tableServiceClient The client for the dataset or snapshot containing the virtual file
   *     system
   * @param datasetTableServiceClient The client for the dataset (which contains the file object
   *     metadata)
   * @param collectionId The ID of the collection in the fsItemFirestore connection that contains
   *     the virtual file system objects
   * @param enumerateDepth how far to enumerate the directory structure
   * @param fireStoreDirectoryEntry The object to enumerate entries within
   * @param context provides either the file id or the file path, for use in error messages.
   * @return An {@link FSItem} representation of the passed in fireStoreDirectoryEntry with nested
   *     FSItems
   */
  private FSItem retrieveWorker(
      TableServiceClient tableServiceClient,
      TableServiceClient datasetTableServiceClient,
      String collectionId,
      int enumerateDepth,
      FireStoreDirectoryEntry fireStoreDirectoryEntry,
      String context) {
    if (fireStoreDirectoryEntry == null) {
      throw new FileNotFoundException("File not found: " + context);
    }

    if (fireStoreDirectoryEntry.getIsFileRef()) {
      FSItem fsFile = makeFSFile(datasetTableServiceClient, collectionId, fireStoreDirectoryEntry);
      if (fsFile == null) {
        // We found a file in the directory that is not done being created. We treat this
        // as not found.
        throw new FileNotFoundException(
            "Found a file, but the directory is not done being created: " + context);
      }
      return fsFile;
    }

    return makeFSDir(
        tableServiceClient,
        datasetTableServiceClient,
        collectionId,
        enumerateDepth,
        fireStoreDirectoryEntry);
  }

  /**
   * Create an FSItem object
   *
   * @param tableServiceClient The client for the dataset or snapshot containing the virtual file
   *     system
   * @param datasetTableServiceClient The client for the dataset (which contains the file object
   *     metadata)
   * @param collectionId The ID of the collection in the fsItemFirestore connection that contains
   *     the virtual file system objects
   * @param level how far to enumerate the directory structure
   * @param fireStoreDirectoryEntry The object to enumerate entries within
   * @return An {@link FSItem} representation of the passed in fireStoreDirectoryEntry with nested
   *     FSItems
   */
  private FSItem makeFSDir(
      TableServiceClient tableServiceClient,
      TableServiceClient datasetTableServiceClient,
      String collectionId,
      int level,
      FireStoreDirectoryEntry fireStoreDirectoryEntry) {
    if (fireStoreDirectoryEntry.getIsFileRef()) {
      throw new IllegalStateException("Expected directory; got file!");
    }

    String fullPath =
        FileMetadataUtils.getFullPath(
            fireStoreDirectoryEntry.getPath(), fireStoreDirectoryEntry.getName());

    FSDir fsDir = new FSDir();
    fsDir
        .fileId(UUID.fromString(fireStoreDirectoryEntry.getFileId()))
        .collectionId(UUID.fromString(collectionId))
        .createdDate(Instant.parse(fireStoreDirectoryEntry.getFileCreatedDate()))
        .path(fullPath)
        .checksumCrc32c(fireStoreDirectoryEntry.getChecksumCrc32c())
        .checksumMd5(fireStoreDirectoryEntry.getChecksumMd5())
        .size(fireStoreDirectoryEntry.getSize())
        .description(StringUtils.EMPTY);

    if (level != 0) {
      List<FSItem> fsContents = new ArrayList<>();
      List<FireStoreDirectoryEntry> dirContents =
          directoryDao.enumerateDirectory(
              tableServiceClient,
              StorageTableName.DATASET.toTableName(UUID.fromString(collectionId)),
              fullPath);
      for (FireStoreDirectoryEntry fso : dirContents) {
        if (fso.getIsFileRef()) {
          // Files that are in the middle of being ingested can have a directory entry, but not yet
          // have
          // a file entry. We do not return files that do not yet have a file entry.
          FSItem fsFile = makeFSFile(datasetTableServiceClient, collectionId, fso);
          if (fsFile != null) {
            fsContents.add(fsFile);
          }
        } else {
          fsContents.add(
              makeFSDir(
                  tableServiceClient, datasetTableServiceClient, collectionId, level - 1, fso));
        }
      }
      fsDir.contents(fsContents);
    }

    return fsDir;
  }

  // Handle files - the fireStoreDirectoryEntry is a reference to a file in a dataset.
  private FSItem makeFSFile(
      TableServiceClient datasetTableServiceClient,
      String datasetId,
      FireStoreDirectoryEntry fireStoreDirectoryEntry) {
    if (!fireStoreDirectoryEntry.getIsFileRef()) {
      throw new IllegalStateException("Expected file; got directory!");
    }

    String fullPath =
        FileMetadataUtils.getFullPath(
            fireStoreDirectoryEntry.getPath(), fireStoreDirectoryEntry.getName());
    String fileId = fireStoreDirectoryEntry.getFileId();

    // Lookup the file in its owning dataset, not in the collection. The collection may be a
    // snapshot directory
    // pointing to the files in one or more datasets.
    FireStoreFile fireStoreFile =
        fileDao.retrieveFileMetadata(datasetTableServiceClient, datasetId, fileId);

    FSFile fsFile = new FSFile();
    fsFile
        .fileId(UUID.fromString(fileId))
        .collectionId(UUID.fromString(datasetId))
        .datasetId(UUID.fromString(fireStoreDirectoryEntry.getDatasetId()))
        .createdDate(Instant.parse(fireStoreFile.getFileCreatedDate()))
        .path(fullPath)
        .checksumCrc32c(fireStoreFile.getChecksumCrc32c())
        .checksumMd5(fireStoreFile.getChecksumMd5())
        .size(fireStoreFile.getSize())
        .description(fireStoreFile.getDescription())
        .cloudPath(fireStoreFile.getGspath())
        .cloudPlatform(CloudPlatform.AZURE)
        .mimeType(fireStoreFile.getMimeType())
        .bucketResourceId(fireStoreFile.getBucketResourceId())
        .loadTag(fireStoreFile.getLoadTag());

    return fsFile;
  }

  public void addFilesToSnapshot(
      TableServiceClient datasetTableServiceClient,
      TableServiceClient snapshotTableServiceClient,
      UUID datasetId,
      String datasetName,
      UUID snapshotId,
      List<String> refIds) {

    directoryDao.addEntriesToSnapshot(
        datasetTableServiceClient,
        snapshotTableServiceClient,
        datasetId,
        datasetName,
        snapshotId,
        refIds);
  }

  // TODO: Implement computeDirectory to recursively compute the size and checksums of a directory

  public StorageTableComputeHelper getHelper(
      TableServiceClient datasetTableServiceClient,
      TableServiceClient snapshotTableServiceClient,
      UUID datasetId,
      UUID snapshotId) {
    return new StorageTableComputeHelper(
        directoryDao,
        fileDao,
        datasetTableServiceClient,
        snapshotTableServiceClient,
        datasetId,
        snapshotId,
        configurationService.getParameterValue(ConfigEnum.AZURE_SNAPSHOT_BATCH_SIZE));
  }

  static class StorageTableComputeHelper implements SnapshotComputeHelper {

    private final TableDirectoryDao directoryDao;
    private final TableFileDao fileDao;
    private final TableServiceClient datasetTableServiceClient;
    private final TableServiceClient snapshotTableServiceClient;
    private final UUID datasetId;
    private final UUID snapshotId;
    private final Integer snapshotBatchSize;
    private final String snapshotTableName;

    StorageTableComputeHelper(
        TableDirectoryDao directoryDao,
        TableFileDao fileDao,
        TableServiceClient datasetTableServiceClient,
        TableServiceClient snapshotTableServiceClient,
        UUID datasetId,
        UUID snapshotId,
        Integer snapshotBatchSize) {
      this.directoryDao = directoryDao;
      this.fileDao = fileDao;
      this.datasetTableServiceClient = datasetTableServiceClient;
      this.snapshotTableServiceClient = snapshotTableServiceClient;
      this.datasetId = datasetId;
      this.snapshotId = snapshotId;
      this.snapshotBatchSize = snapshotBatchSize;
      this.snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    }

    @Override
    public List<FireStoreFile> batchRetrieveFileMetadata(
        Map.Entry<String, List<FireStoreDirectoryEntry>> entry) {
      return fileDao.batchRetrieveFileMetadata(
          datasetTableServiceClient, datasetId.toString(), entry.getValue());
    }

    @Override
    public List<FireStoreDirectoryEntry> enumerateDirectory(String dirPath) {
      return directoryDao.enumerateDirectory(
          snapshotTableServiceClient, snapshotTableName, dirPath);
    }

    @Override
    public void updateEntry(
        FireStoreDirectoryEntry entry, List<FireStoreDirectoryEntry> updateBatch) {
      updateBatch.add(entry);
      if (updateBatch.size() >= snapshotBatchSize) {
        logger.info("Snapshot compute updating batch of {} directory entries", snapshotBatchSize);
        directoryDao.batchStoreDirectoryEntry(snapshotTableServiceClient, snapshotId, updateBatch);
        updateBatch.clear();
      }
    }
  }
}
