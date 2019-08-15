package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
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
public class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreFileDao");

    private FireStoreUtils fireStoreUtils;

    @Autowired
    public FireStoreFileDao(FireStoreUtils fireStoreUtils) {
        this.fireStoreUtils = fireStoreUtils;
    }

    public void createFileMetadata(Dataset dataset, FSFile fsFile) {
        FireStoreFile newFile = makeFireStoreFileFromFSFile(fsFile);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            xn.set(getFileDocRef(dataset, newFile.getObjectId()), newFile);
            return null;
        });

        fireStoreUtils.transactionGet("createFileMetadata", transaction);
    }

    public boolean deleteFileMetadata(Dataset dataset, String fileObjectId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Boolean> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(dataset, fileObjectId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return false;
            }

            xn.delete(docSnap.getReference());
            return true;
        });

        return fireStoreUtils.transactionGet("deleteFileMetadata", transaction);
    }

    // Returns null on not found
    public FSFile retrieveFileMetadata(Dataset dataset, String fileObjectId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<FSFile> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByFileId(dataset, fileObjectId, xn);
            if (docSnap == null || !docSnap.exists()) {
                return null;
            }
            FireStoreFile fireStoreFile = docSnap.toObject(FireStoreFile.class);
            return makeFSFileFromFireStoreFile(dataset, fireStoreFile);
        });

        return fireStoreUtils.transactionGet("retrieveFileMetadata", transaction);
    }

    private DocumentSnapshot lookupByFileId(Dataset dataset, String fileObjectId, Transaction xn) {
        DocumentReference docRef = getFileDocRef(dataset, fileObjectId);
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

    private DocumentReference getFileDocRef(Dataset dataset, String objectId) {
        String collectionName = dataset.getId().toString() + "-files";
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        return fireStoreProject.getFirestore()
            .collection(collectionName)
            .document(objectId);
    }

}
