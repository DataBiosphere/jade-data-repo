package bio.terra.service.filedata.google.firestore;

import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * FireStoreFileDao provides CRUD operations on the file collection in Firestore.
 * Objects in the file collection are referred to by the dataset (owner of the files)
 * and by any snapshots that reference the files. File naming is handled by the directory DAO.
 * This DAO just handles the basic operations for managing the collection. It does not have
 * logic for protecting against deleting files that have dependencies or creating files
 * with duplicate paths.
 *
 * Each write operation is performed in a FireStore transaction.
 */
@Component
class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger(FireStoreFileDao.class);

    private FireStoreUtils fireStoreUtils;

    @Autowired
    FireStoreFileDao(FireStoreUtils fireStoreUtils) {
        this.fireStoreUtils = fireStoreUtils;
    }

    void createFileMetadata(Firestore firestore, String datasetId, FireStoreFile newFile) {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            xn.set(getFileDocRef(firestore, collectionId, newFile.getFileId()), newFile);
            return null;
        });

        fireStoreUtils.transactionGet("createFileMetadata", transaction);
    }

    boolean deleteFileMetadata(Firestore firestore, String datasetId, String fileId) {
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

    // Returns null on not found
    FireStoreFile retrieveFileMetadata(Firestore firestore, String datasetId, String fileId) {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<FireStoreFile> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return null;
            }
            return docSnap.toObject(FireStoreFile.class);
        });

        return fireStoreUtils.transactionGet("retrieveFileMetadata", transaction);
    }

    private DocumentSnapshot lookupByFileId(Firestore firestore,
                                            String collectionId,
                                            String fileId,
                                            Transaction xn) {
        DocumentReference docRef = getFileDocRef(firestore, collectionId, fileId);
        ApiFuture<DocumentSnapshot> docSnapFuture = xn.get(docRef);
        try {
            return docSnapFuture.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("lookupByFileId - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookupByFileId - execution exception", ex);
        }
    }

    // See comment in FireStoreUtils.java for an explanation of the batch size setting.
    private static final int DELETE_BATCH_SIZE = 500;
    void deleteFilesFromDataset(Firestore firestore, String datasetId, Consumer<FireStoreFile> func) {
        String collectionId = makeCollectionId(datasetId);
        fireStoreUtils.scanCollectionObjects(
            firestore,
            collectionId,
            DELETE_BATCH_SIZE,
            document -> {
                FireStoreFile fireStoreFile = document.toObject(FireStoreFile.class);
                func.accept(fireStoreFile);
                document.getReference().delete();
            });
    }

    private String makeCollectionId(String datasetId) {
        return datasetId + "-files";
    }

    private DocumentReference getFileDocRef(Firestore firestore, String collectionId, String fileId) {
        return firestore.collection(collectionId).document(fileId);
    }

}
