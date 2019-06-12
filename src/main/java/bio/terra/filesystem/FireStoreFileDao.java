package bio.terra.filesystem;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSEnumDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
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

import static bio.terra.metadata.FSObjectType.DELETING_FILE;
import static bio.terra.metadata.FSObjectType.FILE;
import static bio.terra.metadata.FSObjectType.INGESTING_FILE;

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
 * We need a root directory to hold the other directories. Since we are doing Firestore lookup by
 * document name, the root directory needs a name. We call it "_dr_"; it could be anything,
 * but it helps when viewing FireStore in the console that it has an obvious name.
 *
 * We don't store the root directory in the paths stored in file and directory objects. It is
 * only used for the Firestore lookup. When we refer to paths in the code we talk about:
 * - lookup path - the path used for the Firestore lookup prepended with "_dr_"
 * - directory path - the directory path to the directory containing object - not including the object name
 * - full path - the full path to the object.
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

    private static final String ROOT_DIR_NAME = "/_dr_";

    private Firestore firestore;
    private FireStoreUtils fireStoreUtils;
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    public FireStoreFileDao(Firestore firestore, FireStoreUtils fireStoreUtils, FireStoreDependencyDao dependencyDao) {
        this.firestore = firestore;
        this.fireStoreUtils = fireStoreUtils;
        this.dependencyDao = dependencyDao;
    }

    public UUID createFileStart(FSFile fileToCreate) {
        if (fileToCreate.getObjectType() != INGESTING_FILE) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }
        UUID studyId = fileToCreate.getStudyId();
        FireStoreObject createObject = makeFileObjectFromFSObject(fileToCreate);

        ApiFuture<UUID> transaction = firestore.runTransaction(xn -> {
            List<FireStoreObject> createList = new ArrayList<>();

            // Walk up the lookup directory path, finding missing directories we get to an existing one
            // We will create the ROOT_DIR_NAME directory here if it does not exist.
            String lookupDirPath = makeLookupPath(getDirectoryPath(fileToCreate.getPath()));

            for (String testPath = lookupDirPath;
                 !testPath.isEmpty();
                 testPath = getDirectoryPath(testPath)) {

                // !!! In this case we are using a lookup path
                DocumentSnapshot docSnap = lookupByObjectPath(studyId.toString(), testPath, xn);
                if (docSnap.exists()) {
                    break;
                }

                FireStoreObject dirToCreate = makeDirectoryObject(
                    studyId,
                    testPath,
                    createObject.getFlightId());

                createList.add(dirToCreate);
            }

            // transition point from reading to writing in the transaction

            for (FireStoreObject dirToCreate : createList) {
                dirToCreate.objectId(UUID.randomUUID().toString());
                xn.set(getDocRef(dirToCreate), dirToCreate);
            }

            UUID objectId = UUID.randomUUID();
            createObject.objectId(objectId.toString());
            xn.set(getDocRef(createObject), createObject);

            return objectId;
        });

        return fireStoreUtils.transactionGet("create start", transaction);
    }

    public boolean createFileStartUndo(String studyId, String fullPath, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            String lookupPath = makeLookupPath(fullPath);
            DocumentSnapshot docSnap = lookupByObjectPath(studyId, fullPath, xn);
            if (!docSnap.exists()) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            // If another flight created this object, then we leave it be.
            if (!StringUtils.equals(flightId, currentObject.getFlightId())) {
                return true;
            }

            // It is ours. Double check that it is in the right state
            if (!StringUtils.equals(INGESTING_FILE.toLetter(),
                currentObject.getObjectTypeLetter())) {
                throw new FileSystemCorruptException("Attempt to createFileStartUndo with bad file object type");
            }

            // transition point from reading to writing is inside of deleteFileWorker
            deleteFileWorker(studyId, docSnap.getReference(), currentObject.getPath(), xn);
            return true;
        });

        return fireStoreUtils.transactionGet("create start undo", transaction);
    }

    public FSObjectBase createFileComplete(FSFileInfo fsFileInfo) {
        ApiFuture<FSObjectBase> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(fsFileInfo.getStudyId(), fsFileInfo.getObjectId(), xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject
                .objectTypeLetter(FILE.toLetter())
                .gspath(fsFileInfo.getGspath())
                .checksumMd5(fsFileInfo.getChecksumMd5())
                .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
                .size(fsFileInfo.getSize())
                .fileCreatedDate(fsFileInfo.getCreatedDate());

            // transition point from reading to writing in the transaction

            xn.set(docSnap.getReference(), currentObject);
            return makeFSObjectFromFileObject(currentObject);
        });

        return fireStoreUtils.transactionGet("create complete", transaction);
    }

    public void createFileCompleteUndo(String studyId, String objectId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                throw new FileSystemCorruptException("File should exist");
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            currentObject.objectTypeLetter(INGESTING_FILE.toLetter());

            // transition point from reading to writing in the transaction

            xn.set(docSnap.getReference(), currentObject);
            return null;
        });

        fireStoreUtils.transactionGet("create complete undo", transaction);
    }

    public boolean deleteFileStart(String studyId, String objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
                case FILE:
                    break;
                case DIRECTORY:
                    throw new InvalidFileSystemObjectTypeException("Invalid attempt to delete a directory");
                case DELETING_FILE:
                    if (!StringUtils.equals(currentObject.getFlightId(), flightId)) {
                        throw new
                            InvalidFileSystemObjectTypeException("File is already being deleted by flight " + flightId);
                    }
                    break;
                case INGESTING_FILE:
                    throw new InvalidFileSystemObjectTypeException("Cannot delete a file that is being ingested");
                default:
                    throw new FileSystemCorruptException("Unknown file system object type");
            }

            if (dependencyDao.objectHasDatasetReference(studyId, objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one dataset and cannot be deleted");
            }

            currentObject
                .objectTypeLetter(DELETING_FILE.toLetter())
                .flightId(flightId);

            // transition point from reading to writing in the transaction

            xn.set(docSnap.getReference(), currentObject);
            return true;
        });

        return fireStoreUtils.transactionGet("delete file start", transaction);
    }

    public boolean deleteFileComplete(String studyId, String objectId, String flightId) {
        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return false;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
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

            if (dependencyDao.objectHasDatasetReference(studyId, objectId)) {
                throw new FileSystemCorruptException("File should not have any references at this point");
            }

            // transition point from reading to writing is inside of deleteFileWorker
            deleteFileWorker(studyId, docSnap.getReference(), currentObject.getPath(), xn);
            return true;
        });

        return fireStoreUtils.transactionGet("delete file start", transaction);
    }

    public void deleteFileStartUndo(String studyId, String objectId, String flightId) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId, objectId, xn);
            if (docSnap == null) {
                return null;
            }

            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);

            switch (FSObjectType.fromLetter(currentObject.getObjectTypeLetter())) {
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

            currentObject.objectTypeLetter(FILE.toLetter());

            // transition point from reading to writing in the transaction
            xn.set(docSnap.getReference(), currentObject);
            return null;
        });

        fireStoreUtils.transactionGet("delete start undo", transaction);
    }

    /**
     * This code is pretty ugly, but here is why...
     * (from https://cloud.google.com/firestore/docs/solutions/delete-collections)
     * <ul>
     * <li>There is no operation that atomically deletes a collection.</li>
     * <li>Deleting a document does not delete the documents in its subcollections.</li>
     * <li>If your documents have dynamic subcollections, (we don't do this!)
     *     it can be hard to know what data to delete for a given path.</li>
     * <li>Deleting a collection of more than 500 documents requires multiple batched
     *     write operations or hundreds of single deletes.</li>
     * </ul>
     *
     * Our objects are small, so I think we can use the maximum batch size without
     * concern for using too much memory.
     *
     * @param studyId
     */
    private static final int DELETE_BATCH_SIZE = 500;
    public void deleteFilesFromStudy(String studyId) {
        CollectionReference studyCollection = firestore.collection(studyId);
        try {
            int batchCount = 0;
            int deleted;
            do {
                deleted = 0;
                ApiFuture<QuerySnapshot> future = studyCollection.limit(DELETE_BATCH_SIZE).get();
                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                batchCount++;
                logger.info("Deleting batch " + batchCount + " of ~" + DELETE_BATCH_SIZE + " documents");
                for (QueryDocumentSnapshot document : documents) {
                    document.getReference().delete();
                    deleted++;
                }
            } while (deleted >= DELETE_BATCH_SIZE);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("delete study - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("delete study - execution exception", ex);
        }
    }

    public FSObjectBase retrieveEnum(UUID studyId, UUID objectId) {
        FSObjectBase fsObject = retrieve(studyId, objectId);
        return enumerateDirectory(fsObject);
    }

    public FSObjectBase retrieve(UUID studyId, UUID objectId) {
        FSObjectBase fsObject = retrieveByIdNoThrow(studyId, objectId);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found. Requested id is: " + objectId);
        }
        return fsObject;
    }

    public FSObjectBase retrieveByIdNoThrow(UUID studyId, UUID objectId) {
        ApiFuture<FSObjectBase> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(studyId.toString(), objectId.toString(), xn);
            if (docSnap == null) {
                return null;
            }
            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            return makeFSObjectFromFileObject(currentObject);
        });

        return fireStoreUtils.transactionGet("retrieve by id", transaction);
    }

    public FSObjectBase retrieveEnumByPath(UUID studyId, String fullPath) {
        FSObjectBase fsObject = retrieveByPath(studyId.toString(), fullPath);
        return enumerateDirectory(fsObject);
    }

    public FSObjectBase retrieveByPath(String studyId, String fullPath) {
        FSObjectBase fsObject = retrieveByPathNoThrow(studyId, fullPath);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found - path: '" + fullPath + "'");
        }
        return fsObject;
    }

    public FSObjectBase retrieveByPathNoThrow(String studyId, String fullPath) {
        String lookupPath = makeLookupPath(fullPath);
        DocumentReference docRef = firestore
            .collection(studyId)
            .document(encodePathAsFirestoreDocumentName(lookupPath));

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

    public List<String> validateRefIds(String studyId, List<String> refIdArray, FSObjectType objectType) {
        List<String> missingIds = new ArrayList<>();
        for (String objectId : refIdArray) {
            if (!lookupByIdAndType(studyId, objectId, objectType)) {
                missingIds.add(objectId);
            }
        }
        return missingIds;
    }

    // -- private methods --

    private void deleteFileWorker(String studyId, DocumentReference fileDocRef, String dirPath, Transaction xn) {
        // We must do all reads before any writes, so we collect the document references that we need to delete
        // first and then perform the deletes afterward. This must be the last part of a transaction that performs
        // a read.
        List<DocumentReference> docRefList = new ArrayList<>();
        docRefList.add(fileDocRef);

        CollectionReference studyCollection = firestore.collection(studyId);

        String lookupPath = makeLookupPath(dirPath);
        try {
            while (!lookupPath.isEmpty()) {
                // Count the number of objects with this path as their directory path
                // A value of 1 means that the directory will be empty after its child is
                // deleted, so we should delete it also.
                Query query = studyCollection.whereEqualTo("path", makePathFromLookupPath(lookupPath));
                ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                if (documents.size() > 1) {
                    break;
                }
                DocumentReference docRef = studyCollection.document(encodePathAsFirestoreDocumentName(lookupPath));
                docRefList.add(docRef);
                lookupPath = getDirectoryPath(lookupPath);
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

    private FSObjectBase enumerateDirectory(FSObjectBase fsObject) {
        if (fsObject.getObjectType() == FSObjectType.FILE) {
            return fsObject;
        }

        ApiFuture<List<FSObjectBase>> transaction = firestore.runTransaction(xn -> {
            Query query = firestore.collection(fsObject.getStudyId().toString())
                .whereEqualTo("path", fsObject.getPath());
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            List<FSObjectBase> fsObjectList = new ArrayList<>();
            for (QueryDocumentSnapshot document : documents) {
                FireStoreObject fireStoreObject = document.toObject(FireStoreObject.class);
                switch (FSObjectType.fromLetter(fireStoreObject.getObjectTypeLetter())) {
                    case FILE:
                    case DIRECTORY:
                        FSObjectBase fsItem = makeFSObjectFromFileObject(fireStoreObject);
                        fsObjectList.add(fsItem);
                        break;

                    // Don't show files being added or deleted
                    case INGESTING_FILE:
                    case DELETING_FILE:
                    default:
                        break;
                }
            }
            return fsObjectList;
        });

        List<FSObjectBase> contents = fireStoreUtils.transactionGet("enumerate directory", transaction);

        return new FSEnumDir()
            .contents(contents)
            .objectId(fsObject.getObjectId())
            .studyId(fsObject.getStudyId())
            .objectType(fsObject.getObjectType())
            .createdDate(fsObject.getCreatedDate())
            .path(fsObject.getPath())
            .size(fsObject.getSize())
            .description(fsObject.getDescription());
    }

    // As mentioned at the top of the module, we can't use forward slash in a FireStore document
    // name, so we do this encoding.
    private static final char DOCNAME_SEPARATOR = '\u001c';
    private String encodePathAsFirestoreDocumentName(String path) {
        return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
    }

    private DocumentReference getDocRef(FireStoreObject fireStoreObject) {
        String lookupPath = makeLookupPath(getFullPath(fireStoreObject));
        return firestore
            .collection(fireStoreObject.getStudyId())
            .document(encodePathAsFirestoreDocumentName(lookupPath));
    }

    private String getObjectName(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length == 0) {
            return StringUtils.EMPTY;
        }
        return pathParts[pathParts.length - 1];
    }

    String getDirectoryPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length <= 1) {
            // We are at the root; no containing directory
            return StringUtils.EMPTY;
        }
        int endIndex = pathParts.length - 1;
        return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
    }

    private String getFullPath(FireStoreObject fireStoreObject) {
        // Originally, this was a method in FireStoreObject, but the Firestore client complained about it,
        // because it was not a set/get for an actual class member. Very picky, that!
        // There are three cases here:
        // - the path and name are empty: that is the root. Full path is "/"
        // - the path is "/" and the name is not empty: dir in the root. Full path is "/name"
        // - the path is "/name" and the name is not empty: Full path is path + "/" + name
        String path = StringUtils.EMPTY;
        if (StringUtils.isNotEmpty(fireStoreObject.getPath()) &&
            !StringUtils.equals(fireStoreObject.getPath(), "/")) {
            path = fireStoreObject.getPath();
        }
        return path + '/' + fireStoreObject.getName();
    }

    private DocumentSnapshot lookupByObjectPath(String studyId, String lookupPath, Transaction xn) {
        try {
            DocumentReference docRef =
                firestore.collection(studyId).document(encodePathAsFirestoreDocumentName(lookupPath));
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

    // Returns true if the object of the requested type exists
    private boolean lookupByIdAndType(String studyId, String objectId, FSObjectType objectType) {
        try {
            CollectionReference studyCollection = firestore.collection(studyId);
            Query query = studyCollection
                .whereEqualTo("objectId", objectId)
                .whereEqualTo("objectTypeLetter", objectType.toLetter());
            ApiFuture<QuerySnapshot> querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            if (documents.size() == 0) {
                return false;
            }
            if (documents.size() != 1) {
                throw new FileSystemCorruptException("lookup by object id found too many objects");
            }

            return true;

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException(" id and type - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException(" id and type - execution exception", ex);
        }
    }

    private FireStoreObject makeDirectoryObject(UUID studyId, String lookupDirPath, String flightId) {
        // We have some special cases to deal with at the top of the directory tree.
        String fullPath = makePathFromLookupPath(lookupDirPath);
        String dirPath = getDirectoryPath(fullPath);
        String objName = getObjectName(fullPath);
        if (StringUtils.isEmpty(fullPath)) {
            // This is the root directory - it doesn't have a path or a name
            dirPath = StringUtils.EMPTY;
            objName = StringUtils.EMPTY;
        } else if (StringUtils.isEmpty(dirPath)) {
            // This is an object in the root directory - it needs to have the root path.
            dirPath = "/";
        }

        return new FireStoreObject()
            .studyId(studyId.toString())
            .objectTypeLetter(FSObjectType.DIRECTORY.toLetter())
            .path(dirPath)
            .name(objName)
            .size(0L)
            .fileCreatedDate(Instant.now().toString())
            .flightId(flightId);
    }

    private FireStoreObject makeFileObjectFromFSObject(FSObjectBase fsObject) {
        String objectId = (fsObject.getObjectId() == null) ? null : fsObject.getObjectId().toString();
        String fileCreatedDate = (fsObject.getCreatedDate() == null) ? null : fsObject.getCreatedDate().toString();

        FireStoreObject fireStoreObject = new FireStoreObject()
            .objectId(objectId)
            .studyId(fsObject.getStudyId().toString())
            .objectTypeLetter(fsObject.getObjectType().toLetter())
            .fileCreatedDate(fileCreatedDate)
            .path(getDirectoryPath(fsObject.getPath()))
            .name(getObjectName(fsObject.getPath()))
            .description(fsObject.getDescription());

        switch (fsObject.getObjectType()) {
            case FILE:
            case DELETING_FILE:
            case INGESTING_FILE:
                FSFile fsFile = (FSFile) fsObject;
                fireStoreObject
                    .gspath(fsFile.getGspath())
                    .checksumCrc32c(fsFile.getChecksumCrc32c())
                    .checksumMd5(fsFile.getChecksumMd5())
                    .mimeType(fsFile.getMimeType())
                    .flightId(fsFile.getFlightId());
                break;

            case DIRECTORY:
                break;

            default:
                throw new FileSystemCorruptException("Invalid object type found");
        }

        return fireStoreObject;
    }

    private FSObjectBase makeFSObjectFromFileObject(FireStoreObject fireStoreObject) {
        Instant createdDate = (fireStoreObject.getFileCreatedDate() == null) ? null :
            Instant.parse(fireStoreObject.getFileCreatedDate());

        FSObjectType objectType = FSObjectType.fromLetter(fireStoreObject.getObjectTypeLetter());
        switch (objectType) {
            case FILE:
            case DELETING_FILE:
            case INGESTING_FILE:
                return new FSFile()
                    .gspath(fireStoreObject.getGspath())
                    .checksumCrc32c(fireStoreObject.getChecksumCrc32c())
                    .checksumMd5(fireStoreObject.getChecksumMd5())
                    .mimeType(fireStoreObject.getMimeType())
                    .flightId(fireStoreObject.getFlightId())
                    // -- base setters --
                    .objectId(UUID.fromString(fireStoreObject.getObjectId()))
                    .studyId(UUID.fromString(fireStoreObject.getStudyId()))
                    .objectType(objectType)
                    .createdDate(createdDate)
                    .path(getFullPath(fireStoreObject))
                    .size(fireStoreObject.getSize())
                    .description(fireStoreObject.getDescription());

            case DIRECTORY:
                return new FSDir()
                    .objectId(UUID.fromString(fireStoreObject.getObjectId()))
                    .studyId(UUID.fromString(fireStoreObject.getStudyId()))
                    .objectType(objectType)
                    .createdDate(createdDate)
                    .path(getFullPath(fireStoreObject))
                    .size(fireStoreObject.getSize())
                    .description(fireStoreObject.getDescription());

            default:
                throw new FileSystemCorruptException("Invalid object type found");
        }
    }

    // Do some tidying of the full path: slash on front - no slash trailing
    // and prepend the root directory name
    private String makeLookupPath(String fullPath) {
        String temp = StringUtils.prependIfMissing(fullPath, "/");
        temp = StringUtils.removeEnd(temp, "/");
        return ROOT_DIR_NAME + temp;
    }

    private String makePathFromLookupPath(String lookupPath) {
        return StringUtils.removeStart(lookupPath, ROOT_DIR_NAME);
    }
}
