package bio.terra.service.filedata.google.firestore;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction;
import com.google.cloud.firestore.WriteResult;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * FireStoreFileDao provides CRUD operations on the file collection in Firestore.
 * Objects in the file collection are referred to by the dataset (owner of the files)
 * and by any snapshots that reference the files. File naming is handled by the directory DAO.
 * This DAO just handles the basic operations for managing the collection. It does not have
 * logic for protecting against deleting files that have dependencies or creating files
 * with duplicate paths.
 * <p>
 * Each write operation is performed in a FireStore transaction.
 */
@Component
class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger(FireStoreFileDao.class);

    private final FireStoreUtils fireStoreUtils;
    private final ConfigurationService configurationService;
    private final ExecutorService executor;

    @Autowired
    FireStoreFileDao(FireStoreUtils fireStoreUtils,
                     ConfigurationService configurationService,
                     @Qualifier("performanceThreadpool") ExecutorService executor) {
        this.fireStoreUtils = fireStoreUtils;
        this.configurationService = configurationService;
        this.executor = executor;
    }

    void createFileMetadata(Firestore firestore, String datasetId, FireStoreFile newFile) throws InterruptedException {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            xn.set(getFileDocRef(firestore, collectionId, newFile.getFileId()), newFile);
            return null;
        });

        fireStoreUtils.transactionGet("createFileMetadata", transaction);
    }

    boolean deleteFileMetadata(Firestore firestore, String datasetId, String fileId) throws InterruptedException {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return false;
            }

            xn.delete(docSnap.getReference());
            return true;
        });

        return fireStoreUtils.transactionGet("deleteFileMetadata", transaction);
    }

    private static final int MAX_RETRIES = 10;
    private static final int RETRY_MILLISECONDS = 500;

    // Returns null on not found
    // We needed to add local retrying to this code path, because it is used in
    // computeDirectory inside of the create snapshot code. We do potentially thousands
    // of these calls inside that step, so retrying at the step level will not work;
    // it would just redo the thousands of calls. Therefore, we retry here. (DR-1307)
    FireStoreFile retrieveFileMetadata(Firestore firestore, String datasetId, String fileId)
        throws InterruptedException {

        int retry = 0;
        while (true) {
            try {
                String collectionId = makeCollectionId(datasetId);
                ApiFuture<FireStoreFile> transaction = firestore.runTransaction(xn -> {
                    DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileId, xn);
                    if (docSnap == null || !docSnap.exists()) {
                        return null;
                    }
                    return docSnap.toObject(FireStoreFile.class);
                });

                // Fault insertion to test retry
                if (configurationService.testInsertFault(ConfigEnum.FIRESTORE_RETRIEVE_FAULT)) {
                    throw new AbortedException(
                        new FileSystemAbortTransactionException("fault insertion"),
                        GrpcStatusCode.of(Status.Code.ABORTED),
                        true);
                }

                return fireStoreUtils.transactionGet("retrieveFileMetadata", transaction);
            } catch (AbortedException ex) {
                if (retry < MAX_RETRIES) {
                    // perform retry
                    retry++;
                    logger.info("Retry retrieveFileMetadata {} of {}", retry, MAX_RETRIES);
                    TimeUnit.MILLISECONDS.sleep(RETRY_MILLISECONDS);
                } else {
                    throw new FileSystemExecutionException("Retries exhausted", ex);
                }
            }
        }
    }

    /**
     * Retrieve metadata from a list of directory entries.
     * @param datasetFirestore A Firestore client
     * @param datasetId The id of the dataset that the directory entries are associated with
     * @param directoryEntries List of objects to retried metadata for
     * @return A list of metadata object for the specified files.  Note: the order of the list matches with the order
     * of the input list objects
     * @throws InterruptedException If a call to Firestore is interrupted
     */
    List<FireStoreFile> batchRetrieveFileMetadata(
        Firestore datasetFirestore,
        String datasetId,
        List<FireStoreDirectoryEntry> directoryEntries) throws InterruptedException {

        CollectionReference collection = datasetFirestore.collection(makeCollectionId(datasetId));

        List<DocumentSnapshot> documentSnapshotList = fireStoreUtils.batchOperation(
            directoryEntries,
            entry -> {
                DocumentReference docRef = collection.document(entry.getFileId());
                return docRef.get();
            });

        List<FireStoreFile> files = new ArrayList<>();
        for (DocumentSnapshot documentSnapshot : documentSnapshotList) {
            if (documentSnapshot == null || !documentSnapshot.exists()) {
                throw new FileSystemCorruptException("Directory entry refers to non-existent file");
            }
            files.add(documentSnapshot.toObject(FireStoreFile.class));
        }

        return files;
    }

    private DocumentSnapshot lookupByFileId(Firestore firestore,
                                            String collectionId,
                                            String fileId,
                                            Transaction xn) throws InterruptedException {
        DocumentReference docRef = getFileDocRef(firestore, collectionId, fileId);
        ApiFuture<DocumentSnapshot> docSnapFuture = xn.get(docRef);
        try {
            return docSnapFuture.get();
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookupByFileId - execution exception", ex);
        }
    }

    // See comment in FireStoreUtils.java for an explanation of the batch size setting.
    private static final int DELETE_BATCH_SIZE = 500;

    void deleteFilesFromDataset(Firestore firestore, String datasetId, InterruptibleConsumer<FireStoreFile> func)
        throws InterruptedException {

        String collectionId = makeCollectionId(datasetId);
        fireStoreUtils.scanCollectionObjects(
            firestore,
            collectionId,
            DELETE_BATCH_SIZE,
            document -> {
                SettableApiFuture<WriteResult> future = SettableApiFuture.create();
                executor.execute(() -> {
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
