package bio.terra.filesystem;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObject;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.Transaction;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Paths and document names
 * FireStore uses forward slash (/) for its path separator. We also use forward slash in our
 * file system paths. To get uniqueness of objects, we want to name objects with their full
 * path. Otherwise, two threads could create the same file as two different documents. That
 * would not do at all.
 *
 * We solve this problem by using document names that replace the forward slash in our paths
 * with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C)
 *
 * Within the document we store the directory path to the object and the object name. That
 * lets us use the indexes to find the objects in a directory using the index.
 *
 * We use FireStore transactions. The required transaction pattern is always read-modify-write.
 * The transactions are expressed as functions that are retried if another transaction touches
 * the object between our transaction's read and write.
 */


@Component
public class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreFileDao");

    private Firestore firestore;
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    public FireStoreFileDao(Firestore firestore, FireStoreDependencyDao dependencyDao) {
        this.firestore = firestore;
        this.dependencyDao = dependencyDao;
    }

    public UUID createFileStart(FSObject fileToCreate) {
        if (fileToCreate.getObjectType() != FSObject.FSObjectType.INGESTING_FILE) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }

        // Walk up the directory path, creating directories as necessary until we get to an existing one
        for (String testPath = getDirectoryPath(fileToCreate.getPath());
             testPath != null;
             testPath = getDirectoryPath(testPath)) {

            FireStoreObject dirToCreate = makeDirectoryObject(fileToCreate.getStudyId(), testPath);
            UUID objectId = createObject(dirToCreate);
            if (objectId == null) {
                break;
            }
        }

        FireStoreObject fireStoreFile = makeFileObjectFromFSObject(fileToCreate);
        return createObject(fireStoreFile);
    }

    public boolean createFileStartUndo(String studyId, String targetPath, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByObjectPath(studyId, targetPath, xn);
            if (!docSnap.exists()) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            if (!StringUtils.equals(flightId, currentObject.getFlightId())) {
                throw new InvalidFileSystemObjectTypeException(
                    "Invalid attempt to delete a file being ingested by a different flight");
            }
            if (!StringUtils.equals(FSObject.FSObjectType.INGESTING_FILE.toLetter(),
                currentObject.getObjectTypeLetter())) {
                throw new FileSystemCorruptException("Attempt to deleteFileForCreateUndo with bad file object type");
            }

            deleteFileWorker(studyId, docSnap.getReference(), currentObject.getPath(), xn);
            return true;
        });

        return transactionGet("create start undo", transaction);
    }

    public FSObject createFileComplete(FSFileInfo fsFileInfo) {
        ApiFuture<FSObject> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(fsFileInfo.getStudyId(), fsFileInfo.getObjectId(), xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject
                .objectTypeLetter(FSObject.FSObjectType.FILE.toLetter())
                .gspath(fsFileInfo.getGspath())
                .checksumMd5(fsFileInfo.getChecksumMd5())
                .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
                .size(fsFileInfo.getSize())
                .fileCreatedDate(fsFileInfo.getCreatedDate());

            xn.set(docSnap.getReference(), currentObject);
            return makeFSObjectFromFileObject(currentObject);
        });

        return transactionGet("create complete", transaction);
    }

    public void createFileCompleteUndo(String studyId, String objectId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject.objectTypeLetter(FSObject.FSObjectType.INGESTING_FILE.toLetter());
            xn.set(docSnap.getReference(), currentObject);
            return null;
        });

        transactionGet("create complete undo", transaction);
    }

    public boolean deleteFileStart(String studyId, String objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case FILE:
                    break;
                case DIRECTORY:
                    throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                    }
                    break;
                case INGESTING_FILE:
                    throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            if (dependencyDao.hasDatasetReference(studyId, objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one dataset and cannot be deleted");
            }

            currentObject
                .objectTypeLetter(FSObject.FSObjectType.DELETING_FILE.toLetter())
                .flightId(flightId);
            xn.set(docSnap.getReference(), currentObject);
            return true;
        });

        return transactionGet("delete file start", transaction);
    }

    public boolean deleteFileComplete(String studyId, String objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case FILE:
                    throw new CorruptMetadataException("File is not marked for deletion");
                case DIRECTORY:
                    throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        throw new InvalidFileSystemObjectTypeException("File is being deleted by someone else");
                    }
                    break;
                case INGESTING_FILE:
                    throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            if (dependencyDao.hasDatasetReference(studyId, objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one dataset and cannot be deleted");
            }

            deleteFileWorker(studyId, docSnap.getReference(), currentObject.getPath(), xn);
            return true;
        });

        return transactionGet("delete file start", transaction);
    }

    public void deleteFileStartUndo(String studyId, String objectId, String flightId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return null;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObject.FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        return null;
                    }
                    break;

                case FILE:
                case DIRECTORY:
                case INGESTING_FILE:
                    return null;
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            currentObject.objectTypeLetter(FSObject.FSObjectType.FILE.toLetter());
            xn.set(docSnap.getReference(), currentObject);
            return null;
        });

        transactionGet("delete start undo", transaction);
    }

    public List<FSObject> enumerateDirectory(UUID studyId, UUID objectId) {
        ApiFuture<List<FSObject>> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                throw new FileSystemObjectNotFoundException("Directory not found");
            }

            FireStoreObject dirObject = docSnap.toObject(FireStoreObject.class);
            if (FSObject.FSObjectType.fromLetter(dirObject.getObjectTypeLetter()) != FSObject.FSObjectType.DIRECTORY) {
                throw new InvalidFileSystemObjectTypeException("Only a directory can be enumerated");
            }

            Query query =  firestore.collection(studyId.toString()).whereEqualTo("path", dirObject.getFullPath());
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            List<FSObject> fsObjectList = new ArrayList<>();
            for (QueryDocumentSnapshot document : documents) {
                FireStoreObject fireStoreObject = document.toObject(FireStoreObject.class);
                FSObject fsObject = makeFSObjectFromFileObject(fireStoreObject);
                fsObjectList.add(fsObject);
            }

            return fsObjectList;
        });

        return transactionGet("enumerate directory", transaction);
    }

    public FSObject retrieve(UUID studyId, UUID objectId) {
        FSObject fsObject = retrieveByIdNoThrow(studyId, objectId);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found. Requested id is: " + objectId);
        }
        return fsObject;
    }

    public FSObject retrieveByIdNoThrow(UUID studyId, UUID objectId) {
        ApiFuture<FSObject> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return null;
            }
            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            return makeFSObjectFromFileObject(currentObject);
        });

        return transactionGet("retrieve by id", transaction);
    }

    public FSObject retrieveByPath(UUID studyId, String path) {
        FSObject fsObject = retrieveByPathNoThrow(studyId, path);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found - path: '" + path + "'");
        }
        return fsObject;
    }

    public FSObject retrieveByPathNoThrow(UUID studyId, String path) {
        DocumentReference docRef = firestore
            .collection(studyId.toString())
            .document(getDocumentName(path));

        ApiFuture<DocumentSnapshot> docSnapFuture = docRef.get();
        try {
            DocumentSnapshot docSnap = docSnapFuture.get();
            if (!docSnap.exists()) {
                return null;
            }
            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            return makeFSObjectFromFileObject(currentObject);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("retrieve - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("retrieve - execution exception", ex);
        }
    }


    // -- private methods --

    // Returns null if object exists; returns new UUID when object is created
    private UUID createObject(FireStoreObject fireStoreObject) {
        DocumentReference docRef = getDocRef(fireStoreObject);

        ApiFuture<UUID> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot snapshot = xn.get(docRef).get();
            if (snapshot.exists()) {
                return null;
            }
            UUID objectId = UUID.randomUUID();
            fireStoreObject.objectId(objectId.toString());
            xn.set(docRef, fireStoreObject);
            return objectId;
        });

        return transactionGet("create directory", transaction);
    }

    private void deleteFileWorker(String studyId, DocumentReference fileDocRef, String path, Transaction xn) {
        // We must do all reads before any writes, so we collect the document references that we need to delete
        // first and then perform the deletes afterward. This must be the last part of a transaction that performs
        // a read.
        List<DocumentReference> docRefList = new ArrayList<>();
        docRefList.add(fileDocRef);

        CollectionReference studyCollection = firestore.collection(studyId);

        try {
            while (path != null) {
                // Count the number of objects with this path as their directory path
                // A value of 1 means that the directory will be empty after its child is
                // deleted, so we should delete it also.
                Query query = studyCollection.whereEqualTo("path", path);
                ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                if (documents.size() > 1) {
                    break;
                }
                DocumentReference docRef = studyCollection.document(getDocumentName(path));
                docRefList.add(docRef);
                path = getDirectoryPath(path);
            }

            for (DocumentReference docRef : docRefList) {
                xn.delete(docRef);
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("delete worker - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("delete worker - execution exception", ex);
        }
    }

    private static final char DOCNAME_SEPARATOR = '\u001c';
    private String getDocumentName(String path) {
        return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
    }

    private DocumentReference getDocRef(FireStoreObject fireStoreObject) {
        return firestore
            .collection(fireStoreObject.getStudyId())
            .document(getDocumentName(fireStoreObject.getFullPath()));
    }

    private String getObjectName(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

    String getDirectoryPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length == 1) {
            // We are at the root; no containing directory
            return null;
        }
        int endIndex = pathParts.length - 1;
        return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
    }

    private DocumentSnapshot lookupByObjectPath(String studyId, String path, Transaction xn) {
        try {
            DocumentReference docRef = firestore.collection(studyId).document(getDocumentName(path));
            ApiFuture<DocumentSnapshot> docSnapFuture = docRef.get();
            return docSnapFuture.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("lookup object path - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookup object path - execution exception", ex);
        }
    }

    // Returns null if not found
    private QueryDocumentSnapshot lookupByObjectId(String studyId, String objectId, Transaction xn) {
        try {
            CollectionReference studyCollection = firestore.collection(studyId);
            Query query = studyCollection.whereEqualTo("objectId", objectId);
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            if (documents.size() == 0) {
                return null;
            }
            if (documents.size() != 1) {
                throw new FileSystemCorruptException("lookup by object id found too many objects");
            }

            return documents.get(0);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("lookup object id - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookup object id - execution exception", ex);
        }
    }

    private FireStoreObject makeDirectoryObject(UUID studyId, String dirPath) {
        return new FireStoreObject()
            .studyId(studyId.toString())
            .objectTypeLetter(FSObject.FSObjectType.DIRECTORY.toLetter())
            .path(getDirectoryPath(dirPath))
            .name(getObjectName(dirPath))
            .size(0L)
            .fileCreatedDate(Instant.now().toString());
    }

    private FireStoreObject makeFileObjectFromFSObject(FSObject fsObject) {
        String objectId = (fsObject.getObjectId() == null) ? null : fsObject.getObjectId().toString();
        return new FireStoreObject()
            .objectId(objectId)
            .studyId(fsObject.getStudyId().toString())
            .objectTypeLetter(fsObject.getObjectType().toLetter())
            .fileCreatedDate(fsObject.getCreatedDate().toString())
            .path(getDirectoryPath(fsObject.getPath()))
            .name(getObjectName(fsObject.getPath()))
            .gspath(fsObject.getGspath())
            .checksumCrc32c(fsObject.getChecksumCrc32c())
            .checksumMd5(fsObject.getChecksumMd5())
            .size(fsObject.getSize())
            .mimeType(fsObject.getMimeType())
            .description(fsObject.getDescription())
            .flightId(fsObject.getFlightId());
    }

    private FSObject makeFSObjectFromFileObject(FireStoreObject fireStoreObject) {
        Instant createdDate = (fireStoreObject.getFileCreatedDate() == null) ? null :
            Instant.parse(fireStoreObject.getFileCreatedDate());

        return new FSObject()
            .objectId(UUID.fromString(fireStoreObject.getObjectId()))
            .studyId(UUID.fromString(fireStoreObject.getStudyId()))
            .objectType(FSObject.FSObjectType.fromLetter(fireStoreObject.getObjectTypeLetter()))
            .createdDate(createdDate)
            .path(fireStoreObject.getFullPath())
            .gspath(fireStoreObject.getGspath())
            .checksumCrc32c(fireStoreObject.getChecksumCrc32c())
            .checksumMd5(fireStoreObject.getChecksumMd5())
            .size(fireStoreObject.getSize())
            .mimeType(fireStoreObject.getMimeType())
            .description(fireStoreObject.getDescription())
            .flightId(fireStoreObject.getFlightId());
    }

    private <T> T transactionGet(String op, ApiFuture<T> transaction) {
        try {
            return transaction.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException(op + " - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException(op + " - execution exception", ex);
        }

    }

}
