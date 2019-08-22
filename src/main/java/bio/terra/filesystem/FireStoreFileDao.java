package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * FireStoreFileDao provides CRUD operations on the file collection in Firestore.
 * Objects in the file collection are referred to by the dataset (owner of the files)
 * and by any snapshots that reference the files. File naming is handled by the directory DAO.
 * This just handles the basic operations for managing the collection. It does not have
 * logic for protecting against deleting files that have dependencies or creating files
 * with duplicate paths.
 *
 * Each write operation is performed in a FireStore transaction.
 */
class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreFileDao");

    private FireStoreUtils fireStoreUtils;

    @Autowired
    public FireStoreFileDao(FireStoreUtils fireStoreUtils) {
        this.fireStoreUtils = fireStoreUtils;
    }

    void createFileMetadata(Firestore firestore, String datasetId, FireStoreFile newFile) {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            xn.set(getFileDocRef(firestore, collectionId, newFile.getObjectId()), newFile);
            return null;
        });

        fireStoreUtils.transactionGet("createFileMetadata", transaction);
    }

    public boolean deleteFileMetadata(Firestore firestore, String datasetId, String fileObjectId) {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(firestore, collectionId, fileObjectId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return false;
            }

            xn.delete(docSnap.getReference());
            return true;
        });

        return fireStoreUtils.transactionGet("deleteFileMetadata", transaction);
    }

    // Returns null on not found
    public FireStoreFile retrieveFileMetadata(Firestore firestore, String datasetId, String fileObjectId) {
        String collectionId = makeCollectionId(datasetId);
        ApiFuture<FireStoreFile> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(firestore, datasetId, fileObjectId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return null;
            }
            return docSnap.toObject(FireStoreFile.class);
        });

        return fireStoreUtils.transactionGet("retrieveFileMetadata", transaction);
    }

    private DocumentSnapshot lookupByFileId(Firestore firestore,
                                            String collectionId,
                                            String fileObjectId,
                                            Transaction xn) {
        DocumentReference docRef = getFileDocRef(firestore, collectionId, fileObjectId);
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






    // This does not make a complete FSFile. Some parts, such as the path, object type and flight id
    // need to be populated from the FireStoreFileRef that refers to this file.
    private FSFile makeFSFileFromFireStoreFile(Dataset dataset, FireStoreFile fireStoreFile) {
        Instant createdDate = (fireStoreFile.getFileCreatedDate() == null) ? null :
            Instant.parse(fireStoreFile.getFileCreatedDate());

        return new FSFile()
            .gspath(fireStoreFile.getGspath())
            .checksumCrc32c(fireStoreFile.getChecksumCrc32c())
            .checksumMd5(fireStoreFile.getChecksumMd5())
            .mimeType(fireStoreFile.getMimeType())
            .profileId(fireStoreFile.getProfileId())
            .region(fireStoreFile.getRegion())
            .bucketResourceId(fireStoreFile.getBucketResourceId())
            // -- base setters have to come after derived setters --
            .objectId(UUID.fromString(fireStoreFile.getObjectId()))
            .datasetId(dataset.getId())
            .createdDate(createdDate)
            .size(fireStoreFile.getSize())
            .description(fireStoreFile.getDescription());
    }

    private FireStoreFile makeFireStoreFileFromFSFile(FSFile fsFile) {
        if ((fsFile.getObjectId() == null) ||
            (fsFile.getDatasetId() == null) ||
            (fsFile.getCreatedDate() == null) ||
            (fsFile.getSize() == null) ||
            (fsFile.getGspath() == null) ||
            (fsFile.getChecksumCrc32c() == null) ||
            (fsFile.getMimeType() == null) ||
            (fsFile.getProfileId() == null) ||
            (fsFile.getRegion() == null) ||
            (fsFile.getBucketResourceId() == null)) {
            throw new FileSystemExecutionException("Invalid FSFile object");
        }

        return new FireStoreFile()
            .objectId(fsFile.getObjectId().toString())
            .fileCreatedDate(fsFile.getCreatedDate().toString())
            .gspath(fsFile.getGspath())
            .checksumCrc32c(fsFile.getChecksumCrc32c())
            .checksumMd5(fsFile.getChecksumMd5())
            .size(fsFile.getSize())
            .mimeType(fsFile.getMimeType())
            .description(fsFile.getDescription())
            .profileId(fsFile.getProfileId())
            .region(fsFile.getRegion())
            .bucketResourceId(fsFile.getBucketResourceId());
    }

    private String makeCollectionId(String datasetId) {
        return datasetId + "-files";
    }

    private DocumentReference getFileDocRef(Firestore firestore, String collectionId, String objectId) {
        return firestore.collection(collectionId).document(objectId);
    }

}
