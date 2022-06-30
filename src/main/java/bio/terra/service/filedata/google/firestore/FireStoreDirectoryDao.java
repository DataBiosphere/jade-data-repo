package bio.terra.service.filedata.google.firestore;

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_QUERY_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_VALIDATE_BATCH_SIZE;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.Transaction;
import com.google.cloud.firestore.WriteResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Paths and document names FireStore uses forward slash (/) for its path separator. We also use
 * forward slash in our file system paths. To get uniqueness of files, we want to name files with
 * their full path. Otherwise, two threads could create the same file as two different documents.
 * That would not do at all.
 *
 * <p>We solve this problem by using FireStore document names that replace the forward slash in our
 * paths with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C) That replacement is
 * <strong>only</strong> used for the FireStore document names. All the other paths we process use
 * the forward slash separator.
 *
 * <p>We need a root directory to hold the other directories. Since we are doing Firestore lookup by
 * document name, the root directory needs a name. We call it "_dr_"; it could be anything, but it
 * helps when viewing FireStore in the console that it has an obvious name.
 *
 * <p>We don't store the root directory in the paths stored in file and directory entries. It is
 * only used for the Firestore lookup. When we refer to paths in the code we talk about: - lookup
 * path - the path used for the Firestore lookup. When building this path (and only this path) we
 * prepended it with "_dr_" as the name for the root directory. - directory path - the directory
 * path to the directory containing entry - not including the entry name - full path - the full path
 * to the entry including the entry name.
 *
 * <p>Within the document we store the directory path to the entry and the entry name. That lets us
 * use the indexes to find the entries in a directory.
 *
 * <p>We use FireStore transactions. The required transaction pattern is always read-modify-write.
 * The transactions are expressed as functions that are retried if another transaction touches the
 * object between our transaction's read and write.
 *
 * <p>It is an invariant that there are no empty directories. When a directory becomes empty on a
 * delete, it is deleted. When a directory is needed, we create it. That is all done within
 * transactions so there is never a time where the externally visible state violates that invariant.
 */
@Component
public class FireStoreDirectoryDao {
  private final Logger logger = LoggerFactory.getLogger(FireStoreDirectoryDao.class);

  private static final int LOOKUP_RETRIES = 30; // up to 5 minutes
  private static final int LOOKUP_WAIT_SECONDS = 10;

  private final FireStoreUtils fireStoreUtils;
  private final PerformanceLogger performanceLogger;
  private final ConfigurationService configurationService;

  @Autowired
  public FireStoreDirectoryDao(
      FireStoreUtils fireStoreUtils,
      PerformanceLogger performanceLogger,
      ConfigurationService configurationService) {
    this.fireStoreUtils = fireStoreUtils;
    this.performanceLogger = performanceLogger;
    this.configurationService = configurationService;
  }

  // Note that this does not test for duplicates. If invoked on an existing path it will overwrite
  // the entry. Existence checking is handled at upper layers.
  public void createDirectoryEntry(
      Firestore firestore, String collectionId, FireStoreDirectoryEntry createEntry)
      throws InterruptedException {

    List<FireStoreDirectoryEntry> createList = new ArrayList<>();

    // Walk up the lookup directory path, finding missing directories we get to an
    // existing one
    // We will create the ROOT_DIR_NAME directory here if it does not exist.
    String lookupDirPath = FileMetadataUtils.makeLookupPath(createEntry.getPath());

    fireStoreUtils.runTransactionWithRetry(
        firestore,
        xn -> {
          for (String testPath = lookupDirPath;
              !testPath.isEmpty();
              testPath = FileMetadataUtils.getDirectoryPath(testPath)) {

            // !!! In this case we are using a lookup path
            DocumentSnapshot docSnap = lookupByFilePath(firestore, collectionId, testPath, xn);
            if (docSnap.exists()) {
              break;
            }

            FireStoreDirectoryEntry dirToCreate = FileMetadataUtils.makeDirectoryEntry(testPath);
            createList.add(dirToCreate);
          }

          // transition point from reading to writing in the transaction
          for (FireStoreDirectoryEntry dirToCreate : createList) {
            xn.set(getDocRef(firestore, collectionId, dirToCreate), dirToCreate);
          }

          xn.set(getDocRef(firestore, collectionId, createEntry), createEntry);
          return null;
        },
        "createFileRef",
        " creating file directory for collection Id: " + collectionId);
  }

  // true - directory entry existed and was deleted; false - directory entry did not exist
  public boolean deleteDirectoryEntry(Firestore firestore, String collectionId, String fileId)
      throws InterruptedException {

    CollectionReference datasetCollection = firestore.collection(collectionId);

    ApiFuture<Boolean> transaction =
        firestore.runTransaction(
            xn -> {
              List<DocumentReference> deleteList = new ArrayList<>();

              // Look up the directory entry by id. If it doesn't exist, we're done
              QueryDocumentSnapshot leafSnap = lookupByFileId(firestore, collectionId, fileId, xn);
              if (leafSnap == null) {
                return false;
              }
              deleteList.add(leafSnap.getReference());

              FireStoreDirectoryEntry leafEntry = leafSnap.toObject(FireStoreDirectoryEntry.class);
              String lookupPath = FileMetadataUtils.makeLookupPath(leafEntry.getPath());
              while (!lookupPath.isEmpty()) {
                // Count the number of entries with this path as their directory path
                // A value of 1 means that the directory will be empty after its child is
                // deleted, so we should delete it also.
                Query query =
                    datasetCollection.whereEqualTo(
                        "path", FileMetadataUtils.makePathFromLookupPath(lookupPath));
                ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                if (documents.size() > 1) {
                  break;
                }
                DocumentReference docRef =
                    datasetCollection.document(encodePathAsFirestoreDocumentName(lookupPath));
                deleteList.add(docRef);
                lookupPath = FileMetadataUtils.getDirectoryPath(lookupPath);
              }

              for (DocumentReference docRef : deleteList) {
                xn.delete(docRef);
              }
              return true;
            });

    return fireStoreUtils.transactionGet("deleteDirectoryEntry", transaction);
  }

  private static final int DELETE_BATCH_SIZE = 500;

  public void deleteDirectoryEntriesFromCollection(Firestore firestore, String collectionId)
      throws InterruptedException {

    fireStoreUtils.scanCollectionObjects(
        firestore, collectionId, DELETE_BATCH_SIZE, document -> document.getReference().delete());
  }

  interface LookupFunction {
    DocumentSnapshot apply(Transaction xn) throws InterruptedException;
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveById(
      Firestore firestore, String collectionId, String fileId) throws InterruptedException {

    DocumentSnapshot docSnap =
        fireStoreUtils.runTransactionWithRetry(
            firestore,
            xn -> lookupByFileId(firestore, collectionId, fileId, xn),
            "retrieveById",
            " file id: " + fileId);

    return Optional.ofNullable(docSnap)
        .map(d -> docSnap.toObject(FireStoreDirectoryEntry.class))
        .orElse(null);
  }

  // Returns null if not found - upper layers do any throwing
  public FireStoreDirectoryEntry retrieveByPath(
      Firestore firestore, String collectionId, String fullPath) throws InterruptedException {

    String lookupPath = FileMetadataUtils.makeLookupPath(fullPath);

    DocumentSnapshot docSnap =
        fireStoreUtils.runTransactionWithRetry(
            firestore,
            xn -> lookupByFilePath(firestore, collectionId, lookupPath, xn),
            "retrieveByPath",
            " path: " + lookupPath);
    return Optional.ofNullable(docSnap)
        .map(d -> docSnap.toObject(FireStoreDirectoryEntry.class))
        .orElse(null);
  }

  public List<String> validateRefIds(
      Firestore firestore, String collectionId, List<String> refIdArray)
      throws InterruptedException {
    List<String> missingIds = new ArrayList<>();
    if (!refIdArray.isEmpty()) {
      int batchSize = configurationService.getParameterValue(FIRESTORE_VALIDATE_BATCH_SIZE);
      List<List<String>> batches = ListUtils.partition(refIdArray, batchSize);
      logger.info(
          "validateRefIds on {} file ids, in {} batches of {}",
          refIdArray.size(),
          batches.size(),
          batchSize);


      for (List<String> batch : batches) {
        List<String> missingBatch = batchValidateIds(firestore, collectionId, batch);
        missingIds.addAll(missingBatch);
      }
    }
    return missingIds;
  }

  // -- private methods --

  List<FireStoreDirectoryEntry> enumerateDirectory(
      Firestore firestore, String collectionId, String dirPath) throws InterruptedException {

    int batchSize = configurationService.getParameterValue(FIRESTORE_QUERY_BATCH_SIZE);
    CollectionReference dirColl = firestore.collection(collectionId);
    Query query = dirColl.whereEqualTo("path", dirPath);
    FireStoreBatchQueryIterator queryIterator =
        new FireStoreBatchQueryIterator(query, batchSize, fireStoreUtils);

    List<FireStoreDirectoryEntry> entryList = new ArrayList<>();
    for (List<QueryDocumentSnapshot> batch = queryIterator.getBatch();
        batch != null;
        batch = queryIterator.getBatch()) {

      for (DocumentSnapshot docSnap : batch) {
        FireStoreDirectoryEntry entry = docSnap.toObject(FireStoreDirectoryEntry.class);
        entryList.add(entry);
      }
    }

    return entryList;
  }

  // As mentioned at the top of the module, we can't use forward slash in a FireStore document
  // name, so we do this encoding.
  private static final char DOCNAME_SEPARATOR = '\u001c';

  public String encodePathAsFirestoreDocumentName(String path) {
    return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
  }

  private DocumentReference getDocRef(
      Firestore firestore, String collectionId, FireStoreDirectoryEntry entry) {
    return getDocRef(firestore, collectionId, entry.getPath(), entry.getName());
  }

  private DocumentReference getDocRef(
      Firestore firestore, String collectionId, String path, String name) {
    String fullPath = FileMetadataUtils.getFullPath(path, name);
    String lookupPath = FileMetadataUtils.makeLookupPath(fullPath);
    return firestore
        .collection(collectionId)
        .document(encodePathAsFirestoreDocumentName(lookupPath));
  }

  private DocumentSnapshot lookupByFilePath(
      Firestore firestore, String collectionId, String lookupPath, Transaction xn)
      throws InterruptedException {
    try {
      DocumentReference docRef =
          firestore
              .collection(collectionId)
              .document(encodePathAsFirestoreDocumentName(lookupPath));
      ApiFuture<DocumentSnapshot> docSnapFuture = xn.get(docRef);
      return docSnapFuture.get();
    } catch (AbortedException | ExecutionException ex) {
      throw fireStoreUtils.handleExecutionException(ex, "lookupByEntryPath");
    }
  }

  // Returns null if not found
  private QueryDocumentSnapshot lookupByFileId(
      Firestore firestore, String collectionId, String fileId, Transaction xn)
      throws InterruptedException {
    try {
      CollectionReference datasetCollection = firestore.collection(collectionId);
      Query query = datasetCollection.whereEqualTo("fileId", fileId);
      ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

      List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
      if (documents.size() == 0) {
        return null;
      }
      if (documents.size() != 1) {
        // TODO: We have seen duplicate documents as a result of concurrency issues.
        //  The query.get() does not appear to be reliably transactional. That may
        //  be a FireStore bug. Regardless, we treat this as a retryable situation.
        //  It *might* be corruption bug on our side. If so, the retry will consistently
        //  fail and eventually give up. When debugging that case, one will have to understand
        //  the purpose of this logic.
        logger.warn(
            "Found too many entries: "
                + documents.size()
                + "; for file: "
                + collectionId
                + "/"
                + fileId);
        throw new FileSystemAbortTransactionException("lookupByFileId found too many entries");
      }

      return documents.get(0);

    } catch (AbortedException | ExecutionException ex) {
      throw fireStoreUtils.handleExecutionException(ex, "lookupByFileId");
    }
  }

  // -- Snapshot filesystem methods --

  // To improve performance of building the snapshot file system, we use three techniques:
  // 1. Operate over batches so that we can issue requests to fire store in parallel
  // 2. Cache directory paths so that we do not due extra lookups or creates on shared directory
  // structure
  // 3. Rewrite rather than read, check existence, and then write. The logic here is that there is
  // no contention
  //    so the writing doesn't generate conflicts, and the typical use case is that overwrites will
  // be rare:
  //      a. File references are usually unique in the datasets we know about
  //      b. Directories are cached, so will be overwritten based on the effectiveness of the cache

  public void addEntriesToSnapshot(
      Firestore datasetFirestore,
      String datasetId,
      String datasetDirName,
      Firestore snapshotFirestore,
      String snapshotId,
      List<String> fileIdList)
      throws InterruptedException {

    int batchSize = configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE);
    List<List<String>> batches = ListUtils.partition(fileIdList, batchSize);
    logger.info(
        "addEntriesToSnapshot on {} file ids, in {} batches of {}",
        fileIdList.size(),
        batches.size(),
        batchSize);

    int cacheSize = configurationService.getParameterValue(ConfigEnum.SNAPSHOT_CACHE_SIZE);
    LRUMap<String, Boolean> pathMap = new LRUMap<>(cacheSize);

    // Create the top directory structure (/_dr_/<datasetDirName>)
    String storeTopTimer = performanceLogger.timerStart();
    storeTopDirectory(snapshotFirestore, snapshotId, datasetDirName);
    performanceLogger.timerEndAndLog(
        storeTopTimer,
        snapshotId,
        this.getClass().getName(),
        "addEntriesToSnapshot:storeTop:" + batchSize);

    int count = 0;
    for (List<String> batch : batches) {
      logger.info("addEntriesToSnapshot batch {}", count);
      count++;

      // Find the file reference dataset entries for all file ids in this batch
      List<FireStoreDirectoryEntry> datasetEntries =
          batchRetrieveById(datasetFirestore, datasetId, batch);

      // Find directory paths that need to be created; plus add to the cache
      Set<String> newPaths = FileMetadataUtils.findNewDirectoryPaths(datasetEntries, pathMap);
      List<FireStoreDirectoryEntry> datasetDirectoryEntries =
          batchRetrieveByPath(datasetFirestore, datasetId, List.copyOf(newPaths));

      // Create snapshot file system entries
      List<FireStoreDirectoryEntry> snapshotEntries = new ArrayList<>();
      for (FireStoreDirectoryEntry datasetEntry : datasetEntries) {
        snapshotEntries.add(datasetEntry.copyEntryUnderNewPath(datasetDirName));
      }
      for (FireStoreDirectoryEntry datasetEntry : datasetDirectoryEntries) {
        snapshotEntries.add(datasetEntry.copyEntryUnderNewPath(datasetDirName));
      }

      // Store the batch of entries. This will override existing entries,
      // but that is not the typical case and it is lower cost just overwrite
      // rather than retrieve to avoid the write.
      batchStoreDirectoryEntry(snapshotFirestore, snapshotId, snapshotEntries);
    }
  }

  private void storeTopDirectory(Firestore firestore, String snapshotId, String dirName)
      throws InterruptedException {
    // We have to create the top directory structure for the dataset and the root folder.
    // Those components cannot be copied from the dataset, but have to be created new
    // in the snapshot directory. We probe to see if the dirName directory exists. If not,
    // we use the createFileRef path to construct it and the parent, if necessary.
    String dirPath = "/" + dirName;
    DocumentSnapshot dirSnap = lookupByPathNoXn(firestore, snapshotId, dirPath);
    if (dirSnap.exists()) {
      return;
    }

    FireStoreDirectoryEntry topDir =
        new FireStoreDirectoryEntry()
            .fileId(UUID.randomUUID().toString())
            .isFileRef(false)
            .path("/")
            .name(dirName)
            .fileCreatedDate(Instant.now().toString());

    createDirectoryEntry(firestore, snapshotId, topDir);
  }

  List<FireStoreDirectoryEntry> batchRetrieveById(
      Firestore firestore, String containerId, List<String> batch) throws InterruptedException {

    CollectionReference collection = firestore.collection(containerId);

    List<QuerySnapshot> querySnapshotList =
        fireStoreUtils.batchOperation(
            batch,
            fileId -> {
              Query query = collection.whereEqualTo("fileId", fileId);
              return query.get();
            });

    List<FireStoreDirectoryEntry> entries = new ArrayList<>();
    for (QuerySnapshot querySnapshot : querySnapshotList) {
      List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
      if (documents.size() != 1) {
        throw new FileSystemExecutionException("FileId not found:");
      }
      QueryDocumentSnapshot docSnap = documents.get(0);
      FireStoreDirectoryEntry entry = docSnap.toObject(FireStoreDirectoryEntry.class);
      if (!entry.getIsFileRef()) {
        throw new FileSystemExecutionException("Directories are not supported as references");
      }
      entries.add(entry);
    }

    return entries;
  }

  private List<FireStoreDirectoryEntry> batchRetrieveByPath(
      Firestore datasetFirestore, String datasetId, List<String> paths)
      throws InterruptedException {

    CollectionReference datasetCollection = datasetFirestore.collection(datasetId);

    List<DocumentSnapshot> documents =
        fireStoreUtils.batchOperation(
            paths,
            path -> {
              DocumentReference docRef =
                  datasetCollection.document(encodePathAsFirestoreDocumentName(path));
              return docRef.get();
            });

    List<FireStoreDirectoryEntry> entries = new ArrayList<>(paths.size());
    for (DocumentSnapshot document : documents) {
      FireStoreDirectoryEntry entry = document.toObject(FireStoreDirectoryEntry.class);
      entries.add(entry);
    }

    return entries;
  }

  // Non-transactional update of a batch of directory entry
  void batchStoreDirectoryEntry(
      Firestore snapshotFirestore, String snapshotId, List<FireStoreDirectoryEntry> entries)
      throws InterruptedException {

    CollectionReference snapshotCollection = snapshotFirestore.collection(snapshotId);

    // We ignore the write results - we don't have any use for them
    fireStoreUtils.batchOperation(
        entries,
        entry -> {
          String fullPath = FileMetadataUtils.getFullPath(entry.getPath(), entry.getName());
          String lookupPath =
              encodePathAsFirestoreDocumentName(FileMetadataUtils.makeLookupPath(fullPath));
          DocumentReference newRef = snapshotCollection.document(lookupPath);
          return newRef.set(entry);
        });
  }

  // We make a special method just for validating id, because when
  // validating we do not need to actually get the data.
  private List<String> batchValidateIds(
      Firestore firestore, String collectionId, List<String> batch) throws InterruptedException {

    CollectionReference datasetCollection = firestore.collection(collectionId);

    List<QuerySnapshot> querySnapshotList =
        fireStoreUtils.batchOperation(
            batch,
            fileId -> {
              Query query = datasetCollection.whereEqualTo("fileId", fileId);
              return query.get();
            });

    List<String> missingIds = new ArrayList<>();
    for (int i = 0; i < batch.size(); i++) {
      QuerySnapshot querySnapshot = querySnapshotList.get(i);
      List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
      if (documents.size() != 1) {
        missingIds.add(batch.get(i));
      }
    }

    return missingIds;
  }

  // Non-transactional update of a directory entry
  void updateDirectoryEntry(Firestore firestore, String collectionId, FireStoreDirectoryEntry entry)
      throws InterruptedException {

    try {
      DocumentReference newRef = getDocRef(firestore, collectionId, entry);
      ApiFuture<WriteResult> writeFuture = newRef.set(entry);
      writeFuture.get();
    } catch (AbortedException | ExecutionException ex) {
      throw fireStoreUtils.handleExecutionException(ex, "updateDirectoryEntry");
    }
  }

  // Non-transactional lookup of an entry
  private DocumentSnapshot lookupByPathNoXn(
      Firestore firestore, String collectionId, String lookupPath) throws InterruptedException {
    DocumentReference docRef =
        firestore.collection(collectionId).document(encodePathAsFirestoreDocumentName(lookupPath));

    RuntimeException lastException = null;
    for (int retryNum = 0; retryNum < LOOKUP_RETRIES; retryNum++) {
      logger.info("FirestoreDirectoryDao lookupByPathNoXn - iteration {}", retryNum);
      try {
        ApiFuture<DocumentSnapshot> docSnapFuture = docRef.get();
        return docSnapFuture.get();
      } catch (AbortedException | ExecutionException ex) {
        lastException = fireStoreUtils.handleExecutionException(ex, "lookupByPathNoXn");
      }
      TimeUnit.SECONDS.sleep(LOOKUP_WAIT_SECONDS);
    }
    throw lastException;
  }
}
