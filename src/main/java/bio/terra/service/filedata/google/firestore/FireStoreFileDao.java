package bio.terra.service.filedata.google.firestore;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction;
import com.google.cloud.firestore.WriteResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * FireStoreFileDao provides CRUD operations on the file collection in Firestore. Objects in the
 * file collection are referred to by the dataset (owner of the files) and by any snapshots that
 * reference the files. File naming is handled by the directory DAO. This DAO just handles the basic
 * operations for managing the collection. It does not have logic for protecting against deleting
 * files that have dependencies or creating files with duplicate paths.
 *
 * <p>Each write operation is performed in a FireStore transaction.
 */
@Component
class FireStoreFileDao {
  private final Logger logger = LoggerFactory.getLogger(FireStoreFileDao.class);

  private final FireStoreUtils fireStoreUtils;
  private final ConfigurationService configurationService;
  private final ExecutorService executor;

  @Autowired
  FireStoreFileDao(
      FireStoreUtils fireStoreUtils,
      ConfigurationService configurationService,
      @Qualifier("performanceThreadpool") ExecutorService executor) {
    this.fireStoreUtils = fireStoreUtils;
    this.configurationService = configurationService;
    this.executor = executor;
  }

  /**
   * Upserts a file metadata object into Firestore (e.g. this is the metadata that contains size,
   * checksum, cloud location etc.) of a file, as opposed to the path information for the file
   */
  void createFileMetadata(Firestore firestore, String datasetId, FireStoreFile newFile)
      throws InterruptedException {
    String collectionId = makeCollectionId(datasetId);
    fireStoreUtils.runTransactionWithRetry(
        firestore,
        xn -> {
          xn.set(getFileDocRef(firestore, collectionId, newFile.getFileId()), newFile);
          return null;
        },
        "createFileMetadata",
        " creating file metadata for dataset Id: " + datasetId);
  }

  /**
   * Upserts file metadata objects into Firestore (e.g. this is the metadata that contains size,
   * checksum, cloud location etc.) of a file, as opposed to the path information for the file
   */
  void createFileMetadata(Firestore firestore, String datasetId, List<FireStoreFile> newFiles)
      throws InterruptedException {
    String collectionId = makeCollectionId(datasetId);
    fireStoreUtils.batchOperation(
        newFiles,
        newFile ->
            firestore.runTransaction(
                xn -> {
                  xn.set(getFileDocRef(firestore, collectionId, newFile.getFileId()), newFile);
                  return null;
                }));
  }

  /** Updates the ID of a file's metadata (effectively, this is a move operation) */
  void moveFileMetadata(Firestore firestore, String datasetId, Map<UUID, UUID> idMappings)
      throws InterruptedException {
    String collectionId = makeCollectionId(datasetId);
    fireStoreUtils.batchOperation(
        new ArrayList<>(idMappings.entrySet()),
        entry ->
            firestore.runTransaction(
                xn -> {
                  // Retrieve the current object
                  FireStoreFile newFile =
                      xn.get(getFileDocRef(firestore, collectionId, entry.getKey().toString()))
                          .get()
                          .toObject(FireStoreFile.class);
                  // Update the ID
                  newFile.fileId(entry.getValue().toString());
                  // Create the new entry
                  xn.set(getFileDocRef(firestore, collectionId, newFile.getFileId()), newFile);
                  // Delete the original entry
                  xn.delete(getFileDocRef(firestore, collectionId, entry.getKey().toString()));
                  return newFile;
                }));
  }

  boolean deleteFileMetadata(Firestore firestore, String datasetId, String fileId)
      throws InterruptedException {
    String collectionId = makeCollectionId(datasetId);
    ApiFuture<Boolean> transaction =
        firestore.runTransaction(
            xn -> {
              DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileId, xn);
              if (docSnap == null || !docSnap.exists()) {
                return false;
              }

              xn.delete(docSnap.getReference());
              return true;
            });

    return fireStoreUtils.transactionGet("deleteFileMetadata", transaction);
  }

  // Returns null on not found
  // We needed to add local retrying to this code path, because it is used in
  // computeDirectory inside of the create snapshot code. We do potentially thousands
  // of these calls inside that step, so retrying at the step level will not work;
  // it would just redo the thousands of calls. Therefore, we retry here. (DR-1307)
  FireStoreFile retrieveFileMetadata(Firestore firestore, String datasetId, String fileId)
      throws InterruptedException {

    String collectionId = makeCollectionId(datasetId);

    return fireStoreUtils.runTransactionWithRetry(
        firestore,
        xn -> {
          DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileId, xn);

          // Fault insertion to test retry
          if (configurationService.testInsertFault(ConfigEnum.FIRESTORE_RETRIEVE_FAULT)) {
            throw new StatusRuntimeException(Status.fromCodeValue(500));
          }

          return Optional.ofNullable(docSnap)
              .map(d -> docSnap.toObject(FireStoreFile.class))
              .orElse(null);
        },
        "retrieveFileMetadata",
        " retrieving file metadata for dataset Id: " + datasetId);
  }

  /**
   * Retrieve metadata from a list of directory entries.
   *
   * @param firestore A Firestore client
   * @param datasetId The id of the dataset that the directory entries are associated with
   * @param directoryEntries List of objects to retried metadata for
   * @return A list of metadata object for the specified files. Note: the order of the list matches
   *     with the order of the input list objects
   * @throws InterruptedException If a call to Firestore is interrupted
   */
  List<FireStoreFile> batchRetrieveFileMetadata(
      Firestore firestore, String datasetId, List<FireStoreDirectoryEntry> directoryEntries)
      throws InterruptedException {

    CollectionReference collection = firestore.collection(makeCollectionId(datasetId));

    List<DocumentSnapshot> documentSnapshotList =
        fireStoreUtils.batchOperation(
            directoryEntries,
            entry -> {
              DocumentReference docRef = collection.document(entry.getFileId());
              return docRef.get();
            });

    List<FireStoreFile> files = new ArrayList<>();
    for (DocumentSnapshot documentSnapshot : documentSnapshotList) {
      if (documentSnapshot == null || !documentSnapshot.exists()) {
        throw new FileSystemCorruptException(
            "Directory entry refers to non-existent file (fileId = %s)"
                .formatted(
                    Optional.ofNullable(documentSnapshot)
                        .map(DocumentSnapshot::getId)
                        .orElse("unknown")));
      }
      files.add(documentSnapshot.toObject(FireStoreFile.class));
    }

    return files;
  }

  /** Enumerate all file entries in a dataset's file collection */
  List<FireStoreFile> enumerateAll(Firestore firestore, String datasetId)
      throws InterruptedException {
    CollectionReference fileColl = firestore.collection(makeCollectionId(datasetId));
    return fireStoreUtils.query(fileColl, FireStoreFile.class);
  }

  private DocumentSnapshot lookupByFileId(
      Firestore firestore, String collectionId, String fileId, Transaction xn)
      throws InterruptedException {
    DocumentReference docRef = getFileDocRef(firestore, collectionId, fileId);
    ApiFuture<DocumentSnapshot> docSnapFuture = xn.get(docRef);
    try {
      return docSnapFuture.get();
    } catch (ExecutionException ex) {
      throw new FileSystemExecutionException("lookupByFileId - execution exception", ex);
    }
  }

  void deleteFilesFromDataset(
      Firestore firestore, String datasetId, InterruptibleConsumer<FireStoreFile> func)
      throws InterruptedException {

    String collectionId = makeCollectionId(datasetId);
    fireStoreUtils.scanCollectionObjectsForDelete(
        firestore,
        collectionId,
        FireStoreUtils.MAX_FIRESTORE_BATCH_SIZE,
        document -> {
          SettableApiFuture<WriteResult> future = SettableApiFuture.create();
          executor.execute(
              () -> {
                try {
                  FireStoreFile fireStoreFile = document.toObject(FireStoreFile.class);
                  func.accept(fireStoreFile);
                  future.set(document.getReference().delete().get());
                } catch (final Exception e) {
                  future.setException(e);
                }
              });
          return future;
        });
  }

  private String makeCollectionId(String datasetId) {
    return datasetId + "-files";
  }

  private DocumentReference getFileDocRef(Firestore firestore, String collectionId, String fileId) {
    return firestore.collection(collectionId).document(fileId);
  }
}
