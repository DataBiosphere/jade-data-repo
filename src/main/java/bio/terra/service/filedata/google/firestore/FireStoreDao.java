package bio.terra.service.filedata.google.firestore;

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.FSDir;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.exception.DirectoryMetadataComputeException;
import bio.terra.service.filedata.exception.FileNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import com.google.cloud.firestore.Firestore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// Operations on a file often need to touch file and directory collections that is,
// the FireStoreFileDao and the FireStoreDirectoryDao.
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
public class FireStoreDao {
  private final Logger logger = LoggerFactory.getLogger(FireStoreDao.class);

  private final FireStoreDirectoryDao directoryDao;
  private final FireStoreFileDao fileDao;
  private final FireStoreUtils fireStoreUtils;
  private final ConfigurationService configurationService;
  private final PerformanceLogger performanceLogger;

  @Autowired
  public FireStoreDao(
      FireStoreDirectoryDao directoryDao,
      FireStoreFileDao fileDao,
      FireStoreUtils fireStoreUtils,
      ConfigurationService configurationService,
      PerformanceLogger performanceLogger) {
    this.directoryDao = directoryDao;
    this.fileDao = fileDao;
    this.fireStoreUtils = fireStoreUtils;
    this.configurationService = configurationService;
    this.performanceLogger = performanceLogger;
  }

  public void createDirectoryEntry(Dataset dataset, FireStoreDirectoryEntry newEntry)
      throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = newEntry.getDatasetId();
    directoryDao.createDirectoryEntry(firestore, datasetId, newEntry);
  }

  public boolean deleteDirectoryEntry(Dataset dataset, String fileId) throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return directoryDao.deleteDirectoryEntry(firestore, datasetId, fileId);
  }

  public void createFileMetadata(Dataset dataset, FireStoreFile newFile)
      throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    fileDao.createFileMetadata(firestore, datasetId, newFile);
  }

  public boolean deleteFileMetadata(Dataset dataset, String fileId) throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return fileDao.deleteFileMetadata(firestore, datasetId, fileId);
  }

  public void deleteFilesFromDataset(Dataset dataset, InterruptibleConsumer<FireStoreFile> func)
      throws InterruptedException {

    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    if (configurationService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
      // If we didn't load files, don't try to delete them
      fileDao.deleteFilesFromDataset(firestore, datasetId, f -> {});
    } else {
      fileDao.deleteFilesFromDataset(firestore, datasetId, func);
    }
    directoryDao.deleteDirectoryEntriesFromCollection(firestore, datasetId);
  }

  public FireStoreDirectoryEntry lookupDirectoryEntry(Dataset dataset, String fileId)
      throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return directoryDao.retrieveById(firestore, datasetId, fileId);
  }

  public FireStoreDirectoryEntry lookupDirectoryEntryByPath(Dataset dataset, String path)
      throws InterruptedException {

    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return directoryDao.retrieveByPath(firestore, datasetId, path);
  }

  public FireStoreFile lookupFile(Dataset dataset, String fileId) throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
  }

  public void addFilesToSnapshot(Dataset dataset, Snapshot snapshot, List<String> refIds)
      throws InterruptedException {

    Firestore datasetFirestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    Firestore snapshotFirestore =
        FireStoreProject.get(snapshot.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    // TODO: Do we need to make sure the dataset name does not contain characters that are invalid
    // for paths?
    // Added the work to figure that out to DR-325
    String datasetName = dataset.getName();
    String snapshotId = snapshot.getId().toString();

    directoryDao.addEntriesToSnapshot(
        datasetFirestore, datasetId, datasetName, snapshotFirestore, snapshotId, refIds);
  }

  public void deleteFilesFromSnapshot(Snapshot snapshot) throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(snapshot.getProjectResource().getGoogleProjectId()).getFirestore();
    String snapshotId = snapshot.getId().toString();
    directoryDao.deleteDirectoryEntriesFromCollection(firestore, snapshotId);
  }

  public void snapshotCompute(Snapshot snapshot) throws InterruptedException {
    Firestore snapshotFirestore =
        FireStoreProject.get(snapshot.getProjectResource().getGoogleProjectId()).getFirestore();
    Firestore datasetFirestore =
        FireStoreProject.get(
                snapshot
                    .getFirstSnapshotSource()
                    .getDataset()
                    .getProjectResource()
                    .getGoogleProjectId())
            .getFirestore();
    String snapshotId = snapshot.getId().toString();
    FireStoreDirectoryEntry topDir =
        directoryDao.retrieveByPath(snapshotFirestore, snapshotId, "/");
    // If topDir is null, it means no files were added to the snapshot file system in the previous
    // step. So there is nothing to compute
    if (topDir != null) {
      // We batch the updates to firestore by collecting updated entries into this list,
      // and when we get enough, writing them out.
      List<FireStoreDirectoryEntry> updateBatch = new ArrayList<>();

      String retrieveTimer = performanceLogger.timerStart();
      computeDirectory(snapshotFirestore, datasetFirestore, snapshotId, topDir, updateBatch);
      performanceLogger.timerEndAndLog(
          retrieveTimer,
          snapshotId, // not a flight, so no job id
          this.getClass().getName(),
          "fireStoreDao.computeDirectoryGetMetadata");

      // Write the last batch out
      directoryDao.batchStoreDirectoryEntry(snapshotFirestore, snapshotId, updateBatch);
    }
  }

  /**
   * Retrieve an FSItem by path
   *
   * @param container - dataset or snapshot containing file's directory entry
   * @param fullPath - path of the file in the directory
   * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all; 1
   *     means contents of this directory; 2 means this and its directories, etc. -1 means the
   *     entire tree.
   * @return FSFile or FSDir of retrieved file; can return null on not found
   */
  public FSItem retrieveByPath(FSContainerInterface container, String fullPath, int enumerateDepth)
      throws InterruptedException {
    Firestore fsItemFirestore =
        FireStoreProject.get(container.getProjectResource().getGoogleProjectId()).getFirestore();
    Firestore metadataFirestore = container.firestoreConnection().getFirestore();
    String containerId = container.getId().toString();

    FireStoreDirectoryEntry fireStoreDirectoryEntry =
        directoryDao.retrieveByPath(fsItemFirestore, containerId, fullPath);
    return retrieveWorker(
        fsItemFirestore,
        metadataFirestore,
        containerId,
        enumerateDepth,
        fireStoreDirectoryEntry,
        fullPath);
  }

  /**
   * Retrieve an FSItem by id
   *
   * @param container - dataset or snapshot containing file's directory entry
   * @param fileId - id of the file or directory
   * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all; 1
   *     means contents of this directory; 2 means this and its directories, etc. -1 means the
   *     entire tree.
   * @return FSFile or FSDir of retrieved file; can return null on not found
   */
  public FSItem retrieveById(FSContainerInterface container, String fileId, int enumerateDepth)
      throws InterruptedException {
    Firestore fsItemFirestore =
        FireStoreProject.get(container.getProjectResource().getGoogleProjectId()).getFirestore();
    Firestore metadataFirestore = container.firestoreConnection().getFirestore();
    String datasetId = container.getId().toString();

    FireStoreDirectoryEntry fireStoreDirectoryEntry =
        directoryDao.retrieveById(fsItemFirestore, datasetId, fileId);
    return retrieveWorker(
        fsItemFirestore,
        metadataFirestore,
        datasetId,
        enumerateDepth,
        fireStoreDirectoryEntry,
        fileId);
  }

  public FSItem retrieveBySnapshotAndId(SnapshotProject snapshot, String fileId, int enumerateDepth)
      throws InterruptedException {
    String projectName = snapshot.getDataProject();
    String datasetId = snapshot.getId().toString();
    Firestore fsItemFirestore = FireStoreProject.get(projectName).getFirestore();
    Firestore metadataFirestore =
        FireStoreProject.get(snapshot.getFirstSourceDatasetProject().getDataProject())
            .getFirestore();

    FireStoreDirectoryEntry fireStoreDirectoryEntry =
        directoryDao.retrieveById(fsItemFirestore, datasetId, fileId);
    return retrieveWorker(
        fsItemFirestore,
        metadataFirestore,
        datasetId,
        enumerateDepth,
        fireStoreDirectoryEntry,
        fileId);
  }

  /**
   * Retrieve a batch of FSFile by id
   *
   * @param container - dataset or snapshot containing file's directory entry
   * @param fileIds - list of ids of file identifiers - directory identifiers will throw
   * @return list of FSItem of retrieved files; throws on not found
   */
  public List<FSFile> batchRetrieveById(
      FSContainerInterface container, List<String> fileIds, int enumerateDepth)
      throws InterruptedException {

    Firestore firestore =
        FireStoreProject.get(container.getProjectResource().getGoogleProjectId()).getFirestore();
    String containerId = container.getId().toString();

    List<FireStoreDirectoryEntry> directoryEntries =
        directoryDao.batchRetrieveById(firestore, containerId, fileIds);

    // TODO: When we have more than one dataset in a snapshot then we will have to
    //  split entries by underlying dataset. For now we know that they all come from one dataset.
    List<FireStoreFile> files =
        fileDao.batchRetrieveFileMetadata(firestore, containerId, directoryEntries);

    List<FSFile> resultList = new ArrayList<>();
    if (directoryEntries.size() != files.size()) {
      throw new FileSystemExecutionException("List sizes should be identical");
    }

    for (int i = 0; i < files.size(); i++) {
      FireStoreFile file = files.get(i);
      FireStoreDirectoryEntry entry = directoryEntries.get(i);

      FSFile fsFile =
          new FSFile()
              .fileId(UUID.fromString(entry.getFileId()))
              .collectionId(UUID.fromString(entry.getDatasetId()))
              .datasetId(UUID.fromString(entry.getDatasetId()))
              .createdDate(Instant.parse(file.getFileCreatedDate()))
              .path(fireStoreUtils.getFullPath(entry.getPath(), entry.getName()))
              .checksumCrc32c(file.getChecksumCrc32c())
              .checksumMd5(file.getChecksumMd5())
              .size(file.getSize())
              .description(file.getDescription())
              .gspath(file.getGspath())
              .mimeType(file.getMimeType())
              .bucketResourceId(file.getBucketResourceId())
              .loadTag(file.getLoadTag());

      resultList.add(fsFile);
    }

    return resultList;
  }

  public List<String> validateRefIds(Dataset dataset, List<String> refIdArray)
      throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String datasetId = dataset.getId().toString();
    return directoryDao.validateRefIds(firestore, datasetId, refIdArray);
  }

  // -- private methods --

  /**
   * Retrieves an FSItem object
   *
   * @param fsItemFirestore The firestore collection that contains the collection with the virtual
   *     file system. This can be a dataset or snapshot project based collection
   * @param metadataFirestore The firestore collection that contains te collection with the file
   *     object metadata. This is always a dataset based project collection
   * @param collectionId The ID of the collection in the fsItemFirestore connection that contains
   *     the virtual file system objects
   * @param enumerateDepth how far to enumerate the directory structure
   * @param fireStoreDirectoryEntry The object to enumerate entries within
   * @param context provides either the file id or the file path, for use in error messages.
   * @return An {@link FSItem} representation of the passed in fireStoreDirectoryEntry with nested
   *     FSItems
   * @throws InterruptedException
   */
  private FSItem retrieveWorker(
      Firestore fsItemFirestore,
      Firestore metadataFirestore,
      String collectionId,
      int enumerateDepth,
      FireStoreDirectoryEntry fireStoreDirectoryEntry,
      String context)
      throws InterruptedException {
    if (fireStoreDirectoryEntry == null) {
      throw new FileNotFoundException("File not found: " + context);
    }

    if (fireStoreDirectoryEntry.getIsFileRef()) {
      FSItem fsFile = makeFSFile(metadataFirestore, collectionId, fireStoreDirectoryEntry);
      if (fsFile == null) {
        // We found a file in the directory that is not done being created. We treat this
        // as not found.
        throw new FileNotFoundException(
            "Found a file, but the directory is not done being created: " + context);
      }
      return fsFile;
    }

    return makeFSDir(
        fsItemFirestore, metadataFirestore, collectionId, enumerateDepth, fireStoreDirectoryEntry);
  }

  /**
   * Create an FSItem object
   *
   * @param fsItemFirestore The firestore collection that contains the collection with the virtual
   *     file system. This can be a dataset or snapshot project based collection
   * @param metadataFirestore The firestore collection that contains te collection with the file
   *     object metadata. This is always a dataset based project collection
   * @param collectionId The ID of the collection in the fsItemFirestore connection that contains
   *     the virtual file system objects
   * @param level how far to enumerate the directory structure
   * @param fireStoreDirectoryEntry The object to enumerate entries within
   * @return An {@link FSItem} representation of the passed in fireStoreDirectoryEntry with nested
   *     FSItems
   * @throws InterruptedException
   */
  private FSItem makeFSDir(
      Firestore fsItemFirestore,
      Firestore metadataFirestore,
      String collectionId,
      int level,
      FireStoreDirectoryEntry fireStoreDirectoryEntry)
      throws InterruptedException {
    if (fireStoreDirectoryEntry.getIsFileRef()) {
      throw new IllegalStateException("Expected directory; got file!");
    }

    String fullPath =
        fireStoreUtils.getFullPath(
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
          directoryDao.enumerateDirectory(fsItemFirestore, collectionId, fullPath);
      for (FireStoreDirectoryEntry fso : dirContents) {
        if (fso.getIsFileRef()) {
          // Files that are in the middle of being ingested can have a directory entry, but not yet
          // have
          // a file entry. We do not return files that do not yet have a file entry.
          FSItem fsFile = makeFSFile(metadataFirestore, collectionId, fso);
          if (fsFile != null) {
            fsContents.add(fsFile);
          }
        } else {
          fsContents.add(
              makeFSDir(fsItemFirestore, metadataFirestore, collectionId, level - 1, fso));
        }
      }
      fsDir.contents(fsContents);
    }

    return fsDir;
  }

  // Handle files - the fireStoreDirectoryEntry is a reference to a file in a dataset.
  private FSItem makeFSFile(
      Firestore datasetFirestore,
      String collectionId,
      FireStoreDirectoryEntry fireStoreDirectoryEntry)
      throws InterruptedException {
    if (!fireStoreDirectoryEntry.getIsFileRef()) {
      throw new IllegalStateException("Expected file; got directory!");
    }

    String fullPath =
        fireStoreUtils.getFullPath(
            fireStoreDirectoryEntry.getPath(), fireStoreDirectoryEntry.getName());
    String fileId = fireStoreDirectoryEntry.getFileId();

    // Lookup the file in its owning dataset, not in the collection. The collection may be a
    // snapshot directory
    // pointing to the files in one or more datasets.
    FireStoreFile fireStoreFile =
        fileDao.retrieveFileMetadata(
            datasetFirestore, fireStoreDirectoryEntry.getDatasetId(), fileId);
    if (fireStoreFile == null) {
      return null;
    }

    FSFile fsFile = new FSFile();
    fsFile
        .fileId(UUID.fromString(fileId))
        .collectionId(UUID.fromString(collectionId))
        .datasetId(UUID.fromString(fireStoreDirectoryEntry.getDatasetId()))
        .createdDate(Instant.parse(fireStoreFile.getFileCreatedDate()))
        .path(fullPath)
        .checksumCrc32c(fireStoreFile.getChecksumCrc32c())
        .checksumMd5(fireStoreFile.getChecksumMd5())
        .size(fireStoreFile.getSize())
        .description(fireStoreFile.getDescription())
        .gspath(fireStoreFile.getGspath())
        .mimeType(fireStoreFile.getMimeType())
        .bucketResourceId(fireStoreFile.getBucketResourceId())
        .loadTag(fireStoreFile.getLoadTag());

    return fsFile;
  }

  // Recursively compute the size and checksums of a directory
  FireStoreDirectoryEntry computeDirectory(
      Firestore snapshotFirestore,
      Firestore datasetFirestore,
      String snapshotId,
      FireStoreDirectoryEntry dirEntry,
      List<FireStoreDirectoryEntry> updateBatch)
      throws InterruptedException {

    String fullPath = fireStoreUtils.getFullPath(dirEntry.getPath(), dirEntry.getName());
    List<FireStoreDirectoryEntry> enumDir =
        directoryDao.enumerateDirectory(snapshotFirestore, snapshotId, fullPath);

    List<FireStoreDirectoryEntry> enumComputed = new ArrayList<>();

    // Recurse to compute results from underlying directories
    try (Stream<FireStoreDirectoryEntry> stream = enumDir.stream()) {
      enumComputed.addAll(
          stream
              .filter(f -> !f.getIsFileRef())
              .map(
                  f -> {
                    try {
                      return computeDirectory(
                          snapshotFirestore, datasetFirestore, snapshotId, f, updateBatch);
                    } catch (InterruptedException e) {
                      throw new DirectoryMetadataComputeException(
                          "Error computing directory metadata", e);
                    }
                  })
              .collect(Collectors.toList()));
    }

    // Collect metadata for file objects in the directory
    try (Stream<FireStoreDirectoryEntry> stream = enumDir.stream()) {
      // Group FireStoreDirectoryEntry objects by dataset Id to process one dataset at a time
      final Map<String, List<FireStoreDirectoryEntry>> fileRefsByDatasetId =
          stream
              .filter(FireStoreDirectoryEntry::getIsFileRef)
              .collect(Collectors.groupingBy(FireStoreDirectoryEntry::getDatasetId));

      for (Map.Entry<String, List<FireStoreDirectoryEntry>> entry :
          fileRefsByDatasetId.entrySet()) {
        // Retrieve the file metadata from Firestore
        final List<FireStoreFile> fireStoreFiles =
            fileDao.batchRetrieveFileMetadata(datasetFirestore, entry.getKey(), entry.getValue());

        final AtomicInteger index = new AtomicInteger(0);
        enumComputed.addAll(
            CollectionUtils.collect(
                entry.getValue(),
                dirItem -> {
                  final FireStoreFile file = fireStoreFiles.get(index.getAndIncrement());
                  if (file == null) {
                    throw new FileNotFoundException("File metadata was missing");
                  }
                  return dirItem
                      .size(file.getSize())
                      .checksumMd5(file.getChecksumMd5())
                      .checksumCrc32c(file.getChecksumCrc32c());
                }));
      }
    }

    // Collect the ingredients for computing this directory's checksums and size
    List<String> md5Collection = new ArrayList<>();
    List<String> crc32cCollection = new ArrayList<>();
    Long totalSize = 0L;

    for (FireStoreDirectoryEntry dirItem : enumComputed) {
      totalSize = totalSize + dirItem.getSize();
      if (!StringUtils.isEmpty(dirItem.getChecksumCrc32c())) {
        crc32cCollection.add(StringUtils.lowerCase(dirItem.getChecksumCrc32c()));
      }
      if (!StringUtils.isEmpty(dirItem.getChecksumMd5())) {
        md5Collection.add(StringUtils.lowerCase(dirItem.getChecksumMd5()));
      }
    }

    // Compute checksums
    // The spec is not 100% clear on the algorithm. I made specific choices on
    // how to implement it:
    // - set hex strings to lowercase before processing so we get consistent sort
    //   order and digest results.
    // - do not make leading zeros converting crc32 long to hex and it is returned
    //   in lowercase. (Matches the semantics of toHexString).
    Collections.sort(md5Collection);
    String md5Concat = StringUtils.join(md5Collection, StringUtils.EMPTY);
    String md5Checksum = fireStoreUtils.computeMd5(md5Concat);

    Collections.sort(crc32cCollection);
    String crc32cConcat = StringUtils.join(crc32cCollection, StringUtils.EMPTY);
    String crc32cChecksum = fireStoreUtils.computeCrc32c(crc32cConcat);

    // Update the directory in place
    dirEntry.checksumCrc32c(crc32cChecksum).checksumMd5(md5Checksum).size(totalSize);
    updateEntry(snapshotFirestore, snapshotId, dirEntry, updateBatch);

    return dirEntry;
  }

  private void updateEntry(
      Firestore firestore,
      String snapshotId,
      FireStoreDirectoryEntry entry,
      List<FireStoreDirectoryEntry> updateBatch)
      throws InterruptedException {

    updateBatch.add(entry);
    int batchSize = configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE);
    if (updateBatch.size() >= batchSize) {
      logger.info("Snapshot compute updating batch of {} directory entries", batchSize);
      directoryDao.batchStoreDirectoryEntry(firestore, snapshotId, updateBatch);
      updateBatch.clear();
    }
  }
}
