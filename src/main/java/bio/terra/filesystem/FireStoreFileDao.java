package bio.terra.filesystem;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.Dataset;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
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
import java.util.function.Consumer;

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
 * We solve this problem by using FireStore document names that replace the forward slash in our paths
 * with character 0x1c - the unicode file separator character. That allows documents to be
 * named with their full names. (See https://www.compart.com/en/unicode/U+001C)  That replacement is
 * <strong>only</strong> used for the FireStore document names. All the other paths we process use
 * the forward slash separator.
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
 *
 * It is an invariant that there are no empty directories. When a directory becomes empty on a delete,
 * it is deleted. When a directory is needed, we create it. That is all done within transactions so
 * there is never a time where the externally visible state violates that invariant.
 */

@Component
public class FireStoreFileDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreFileDao");

    private static final String ROOT_DIR_NAME = "/_dr_";

    private FireStoreUtils fireStoreUtils;
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    public FireStoreFileDao(FireStoreUtils fireStoreUtils, FireStoreDependencyDao dependencyDao) {
        this.fireStoreUtils = fireStoreUtils;
        this.dependencyDao = dependencyDao;
    }

    public UUID createFileStart(Dataset dataset, FSFile fileToCreate) {
        if (fileToCreate.getObjectType() != INGESTING_FILE) {
            throw new InvalidFileSystemObjectTypeException("Invalid file system object type");
        }
        FireStoreObject createObject = makeFireStoreObjectFromFSObject(fileToCreate);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<UUID> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            List<FireStoreObject> createList = new ArrayList<>();

            // Walk up the lookup directory path, finding missing directories we get to an existing one
            // We will create the ROOT_DIR_NAME directory here if it does not exist.
            String lookupDirPath = makeLookupPath(getDirectoryPath(fileToCreate.getPath()));

            for (String testPath = lookupDirPath;
                 !testPath.isEmpty();
                 testPath = getDirectoryPath(testPath)) {

                // !!! In this case we are using a lookup path
                DocumentSnapshot docSnap = lookupByObjectPath(dataset, testPath, xn);
                if (docSnap.exists()) {
                    break;
                }

                FireStoreObject dirToCreate = makeDirectoryObject(
                    dataset,
                    testPath,
                    createObject.getFlightId());

                createList.add(dirToCreate);
            }

            // transition point from reading to writing in the transaction

            for (FireStoreObject dirToCreate : createList) {
                dirToCreate.objectId(UUID.randomUUID().toString());
                xn.set(getDocRef(dataset, dirToCreate), dirToCreate);
            }

            UUID objectId = UUID.randomUUID();
            createObject.objectId(objectId.toString());
            xn.set(getDocRef(dataset, createObject), createObject);

            return objectId;
        });

        return fireStoreUtils.transactionGet("create start", transaction);
    }

    public void createFileStartUndo(Dataset dataset, String fullPath, String flightId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            String lookupPath = makeLookupPath(fullPath);
            DocumentSnapshot docSnap = lookupByObjectPath(dataset, lookupPath, xn);
            if (docSnap.exists()) {
                FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
                // If another flight created this object, then we leave it be.
                if (StringUtils.equals(flightId, currentObject.getFlightId())) {
                    // It is ours. Double check that it is in the right state
                    if (!StringUtils.equals(INGESTING_FILE.toLetter(), currentObject.getObjectTypeLetter())) {
                        throw new
                            FileSystemCorruptException("Attempt to createFileStartUndo with bad file object type");
                    }

                    // transition point from reading to writing is inside of deleteFileWorker
                    deleteFileWorker(dataset, docSnap.getReference(), currentObject.getPath(), xn);
                }
            }
            return null;
        });

        fireStoreUtils.transactionGet("create start undo", transaction);
    }

    public FSObjectBase createFileComplete(Dataset dataset, FSFileInfo fsFileInfo) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<FSObjectBase> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, fsFileInfo.getObjectId(), xn);
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
                .fileCreatedDate(fsFileInfo.getCreatedDate())
                .region(fsFileInfo.getRegion())
                .bucketResourceId(fsFileInfo.getBucketResourceId());

            // transition point from reading to writing in the transaction

            xn.set(docSnap.getReference(), currentObject);
            return makeFSObjectFromFireStoreObject(currentObject);
        });

        return fireStoreUtils.transactionGet("create complete", transaction);
    }

    public void createFileCompleteUndo(Dataset dataset, String objectId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, objectId, xn);
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

    public boolean deleteFileStart(Dataset dataset, String objectId, String flightId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Boolean> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, objectId, xn);
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

            if (dependencyDao.objectHasSnapshotReference(dataset, objectId)) {
                throw new FileSystemObjectDependencyException(
                    "File is used by at least one snapshot and cannot be deleted");
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

    public boolean deleteFileComplete(Dataset dataset, String objectId, String flightId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Boolean> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, objectId, xn);
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

            if (dependencyDao.objectHasSnapshotReference(dataset, objectId)) {
                throw new FileSystemCorruptException("File should not have any references at this point");
            }

            // transition point from reading to writing is inside of deleteFileWorker
            deleteFileWorker(dataset, docSnap.getReference(), currentObject.getPath(), xn);
            return true;
        });

        return fireStoreUtils.transactionGet("delete file start", transaction);
    }

    public void deleteFileStartUndo(Dataset dataset, String objectId, String flightId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, objectId, xn);
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
     * @param datasetId
     */
    private static final int BATCH_SIZE = 500;
    public void deleteFilesFromDataset(Dataset dataset) {
        visitEachDocumentInDataset(dataset, document -> document.getReference().delete());
    }

    public void forEachFsObjectInDataset(Dataset dataset, Consumer<FSObjectBase> func) {
        visitEachDocumentInDataset(dataset, document -> {
            FireStoreObject currentObject = document.toObject(FireStoreObject.class);
            FSObjectBase fsObject = makeFSObjectFromFireStoreObject(currentObject);
            func.accept(fsObject);
        });
    }

    private void visitEachDocumentInDataset(Dataset dataset, Consumer<QueryDocumentSnapshot> func) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        CollectionReference datasetCollection = fireStoreProject.getFirestore().collection(dataset.getId().toString());
        try {
            int batchCount = 0;
            int visited;
            do {
                visited = 0;
                ApiFuture<QuerySnapshot> future = datasetCollection.limit(BATCH_SIZE).get();
                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                batchCount++;
                logger.info("Visiting batch " + batchCount + " of ~" + BATCH_SIZE + " documents");
                for (QueryDocumentSnapshot document : documents) {
                    func.accept(document);
                    visited++;
                }
            } while (visited >= BATCH_SIZE);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("scanning dataset - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("scanning dataset - execution exception", ex);
        }
    }

    public FSObjectBase retrieveWithContents(Dataset dataset, UUID objectId) {
        FSObjectBase fsObject = retrieve(dataset, objectId);
        return enumerateDirectory(dataset, fsObject);
    }

    public FSObjectBase retrieve(Dataset dataset, UUID objectId) {
        FSObjectBase fsObject = retrieveByIdNoThrow(dataset, objectId);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found. Requested id is: " + objectId);
        }
        return fsObject;
    }

    public FSObjectBase retrieveByIdNoThrow(Dataset dataset, UUID objectId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<FSObjectBase> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(dataset, objectId.toString(), xn);
            if (docSnap == null) {
                return null;
            }
            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            return makeFSObjectFromFireStoreObject(currentObject);
        });

        return fireStoreUtils.transactionGet("retrieve by id", transaction);
    }

    public FSObjectBase retrieveWithContentsByPath(Dataset dataset, String fullPath) {
        FSObjectBase fsObject = retrieveByPath(dataset, fullPath);
        return enumerateDirectory(dataset, fsObject);
    }

    public FSObjectBase retrieveByPath(Dataset dataset, String fullPath) {
        FSObjectBase fsObject = retrieveByPathNoThrow(dataset, fullPath);
        if (fsObject == null) {
            throw new FileSystemObjectNotFoundException("Object not found - path: '" + fullPath + "'");
        }
        return fsObject;
    }

    public FSObjectBase retrieveByPathNoThrow(Dataset dataset, String fullPath) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String lookupPath = makeLookupPath(fullPath);
        DocumentReference docRef = fireStoreProject.getFirestore()
            .collection(dataset.getId().toString())
            .document(encodePathAsFirestoreDocumentName(lookupPath));

        ApiFuture<DocumentSnapshot> docSnapFuture = docRef.get();
        try {
            DocumentSnapshot docSnap = docSnapFuture.get();
            if (!docSnap.exists()) {
                return null;
            }
            FireStoreObject currentObject = docSnap.toObject(FireStoreObject.class);
            return makeFSObjectFromFireStoreObject(currentObject);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("retrieve - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("retrieve - execution exception", ex);
        }
    }

    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray, FSObjectType objectType) {
        List<String> missingIds = new ArrayList<>();
        for (String objectId : refIdArray) {
            if (!lookupByIdAndType(dataset, objectId, objectType)) {
                missingIds.add(objectId);
            }
        }
        return missingIds;
    }

    // -- private methods --

    private void deleteFileWorker(Dataset dataset, DocumentReference fileDocRef, String dirPath, Transaction xn) {
        // We must do all reads before any writes, so we collect the document references that we need to delete
        // first and then perform the deletes afterward. This must be the last part of a transaction that performs
        // a read.
        List<DocumentReference> docRefList = new ArrayList<>();
        docRefList.add(fileDocRef);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        CollectionReference datasetCollection = fireStoreProject.getFirestore().collection(dataset.getId().toString());

        String lookupPath = makeLookupPath(dirPath);
        try {
            while (!lookupPath.isEmpty()) {
                // Count the number of objects with this path as their directory path
                // A value of 1 means that the directory will be empty after its child is
                // deleted, so we should delete it also.
                Query query = datasetCollection.whereEqualTo("path", makePathFromLookupPath(lookupPath));
                ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);

                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                if (documents.size() > 1) {
                    break;
                }
                DocumentReference docRef = datasetCollection.document(encodePathAsFirestoreDocumentName(lookupPath));
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

    private FSObjectBase enumerateDirectory(Dataset dataset, FSObjectBase fsObject) {
        if (fsObject.getObjectType() == FSObjectType.FILE) {
            return fsObject;
        }
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        ApiFuture<List<FSObjectBase>> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            Query query = fireStoreProject.getFirestore().collection(fsObject.getDatasetId().toString())
                .whereEqualTo("path", fsObject.getPath());
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            List<FSObjectBase> fsObjectList = new ArrayList<>();
            for (QueryDocumentSnapshot document : documents) {
                FireStoreObject fireStoreObject = document.toObject(FireStoreObject.class);
                switch (FSObjectType.fromLetter(fireStoreObject.getObjectTypeLetter())) {
                    case FILE:
                    case DIRECTORY:
                        FSObjectBase fsItem = makeFSObjectFromFireStoreObject(fireStoreObject);
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

        return new FSDir()
            .contents(contents)
            .objectId(fsObject.getObjectId())
            .datasetId(fsObject.getDatasetId())
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

    private DocumentReference getDocRef(Dataset dataset, FireStoreObject fireStoreObject) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String lookupPath = makeLookupPath(getFullPath(fireStoreObject));
        return fireStoreProject.getFirestore()
            .collection(fireStoreObject.getDatasetId())
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

    private DocumentSnapshot lookupByObjectPath(Dataset dataset, String lookupPath, Transaction xn) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        try {
            DocumentReference docRef =
                fireStoreProject.getFirestore().collection(dataset.getId().toString())
                    .document(encodePathAsFirestoreDocumentName(lookupPath));
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
    private QueryDocumentSnapshot lookupByObjectId(Dataset dataset, String objectId, Transaction xn) {
        try {
            FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
            CollectionReference datasetCollection =
                fireStoreProject.getFirestore().collection(dataset.getId().toString());
            Query query = datasetCollection.whereEqualTo("objectId", objectId);
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
    private boolean lookupByIdAndType(Dataset dataset, String objectId, FSObjectType objectType) {
        try {
            FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
            CollectionReference datasetCollection =
                fireStoreProject.getFirestore().collection(dataset.getId().toString());
            Query query = datasetCollection
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

    private FireStoreObject makeDirectoryObject(Dataset dataset, String lookupDirPath, String flightId) {
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
            .datasetId(dataset.getId().toString())
            .objectTypeLetter(FSObjectType.DIRECTORY.toLetter())
            .path(dirPath)
            .name(objName)
            .size(0L)
            .fileCreatedDate(Instant.now().toString())
            .flightId(flightId);
    }

    private FireStoreObject makeFireStoreObjectFromFSObject(FSObjectBase fsObject) {
        String objectId = (fsObject.getObjectId() == null) ? null : fsObject.getObjectId().toString();
        String fileCreatedDate = (fsObject.getCreatedDate() == null) ? null : fsObject.getCreatedDate().toString();

        FireStoreObject fireStoreObject = new FireStoreObject()
            .objectId(objectId)
            .datasetId(fsObject.getDatasetId().toString())
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
                    .flightId(fsFile.getFlightId())
                    .profileId(fsFile.getProfileId())
                    .region(fsFile.getRegion())
                    .bucketResourceId(fsFile.getBucketResourceId());
                break;

            case DIRECTORY:
                break;

            default:
                throw new FileSystemCorruptException("Invalid object type found");
        }

        return fireStoreObject;
    }

    private FSObjectBase makeFSObjectFromFireStoreObject(FireStoreObject fireStoreObject) {
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
                    .profileId(fireStoreObject.getProfileId())
                    .region(fireStoreObject.getRegion())
                    .bucketResourceId(fireStoreObject.getBucketResourceId())
                    // -- base setters --
                    .objectId(UUID.fromString(fireStoreObject.getObjectId()))
                    .datasetId(UUID.fromString(fireStoreObject.getDatasetId()))
                    .objectType(objectType)
                    .createdDate(createdDate)
                    .path(getFullPath(fireStoreObject))
                    .size(fireStoreObject.getSize())
                    .description(fireStoreObject.getDescription());

            case DIRECTORY:
                return new FSDir()
                    .objectId(UUID.fromString(fireStoreObject.getObjectId()))
                    .datasetId(UUID.fromString(fireStoreObject.getDatasetId()))
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
