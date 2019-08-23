package bio.terra.filesystem;

import bio.terra.dao.exception.CorruptMetadataException;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemExecutionException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
 * - full path - the full path to the object including the object name.
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
public class FireStoreDirectoryDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreDirectoryDao");

    private static final String ROOT_DIR_NAME = "/_dr_";

    private FireStoreUtils fireStoreUtils;
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    public FireStoreDirectoryDao(FireStoreUtils fireStoreUtils, FireStoreDependencyDao dependencyDao) {
        this.fireStoreUtils = fireStoreUtils;
        this.dependencyDao = dependencyDao;
    }

    public void createFileRef(Firestore firestore, String collectionId, FireStoreObject createObject) {
        ApiFuture<Void> transaction = firestore.runTransaction(xn -> {
            List<FireStoreObject> createList = new ArrayList<>();

            // Walk up the lookup directory path, finding missing directories we get to an existing one
            // We will create the ROOT_DIR_NAME directory here if it does not exist.
            String lookupDirPath = makeLookupPath(createObject.getPath());

            for (String testPath = lookupDirPath;
                 !testPath.isEmpty();
                 testPath = fireStoreUtils.getDirectoryPath(testPath)) {

                // !!! In this case we are using a lookup path
                DocumentSnapshot docSnap = lookupByObjectPath(firestore, collectionId, testPath, xn);
                if (docSnap.exists()) {
                    break;
                }

                FireStoreObject dirToCreate = makeDirectoryObject(collectionId, testPath);
                createList.add(dirToCreate);
            }

            // transition point from reading to writing in the transaction

            for (FireStoreObject dirToCreate : createList) {
                xn.set(getDocRef(firestore, collectionId, dirToCreate), dirToCreate);
            }

            xn.set(getDocRef(firestore, collectionId, createObject), createObject);
            return null;
        });

        fireStoreUtils.transactionGet("createFileRef", transaction);
    }

    // true - object existed and was deleted; false - object did not exist
    public boolean deleteFileRef(Firestore firestore, String collectionId, String objectId) {
        CollectionReference datasetCollection = firestore.collection(collectionId);

        ApiFuture<Boolean> transaction = firestore.runTransaction(xn -> {
            List<DocumentReference> deleteList = new ArrayList<>();

            // Look up the object by id. If it doesn't exist, we're done
            QueryDocumentSnapshot leafSnap = lookupByObjectId(firestore, collectionId, objectId, xn);
            if (leafSnap == null) {
                return false;
            }
            deleteList.add(leafSnap.getReference());

            FireStoreObject leafObject = leafSnap.toObject(FireStoreObject.class);
            String lookupPath = makeLookupPath(leafObject.getPath());
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
                deleteList.add(docRef);
                lookupPath = fireStoreUtils.getDirectoryPath(lookupPath);
            }

            for (DocumentReference docRef : deleteList) {
                xn.delete(docRef);
            }
            return true;
        });

        return fireStoreUtils.transactionGet("deleteFileRef", transaction);
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
    private static final int DELETE_BATCH_SIZE = 500;
    public void deleteFilesFromDataset(Dataset dataset) {
        visitEachDocumentInDataset(dataset, DELETE_BATCH_SIZE, document -> document.getReference().delete());
    }

    public void forEachFsObjectInDataset(Dataset dataset, int batchSize, Consumer<FSObjectBase> func) {
        visitEachDocumentInDataset(dataset, batchSize, document -> {
            FireStoreObject currentObject = document.toObject(FireStoreObject.class);
            FSObjectBase fsObject = makeFSObjectFromFireStoreObject(currentObject);
            func.accept(fsObject);
        });
    }

    private void visitEachDocumentInDataset(Dataset dataset, int batchSize, Consumer<QueryDocumentSnapshot> func) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        CollectionReference datasetCollection = fireStoreProject.getFirestore().collection(dataset.getId().toString());
        try {
            int batchCount = 0;
            int visited;
            do {
                visited = 0;
                ApiFuture<QuerySnapshot> future = datasetCollection.limit(batchSize).get();
                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                batchCount++;
                logger.info("Visiting batch " + batchCount + " of ~" + batchSize + " documents");
                for (QueryDocumentSnapshot document : documents) {
                    func.accept(document);
                    visited++;
                }
            } while (visited >= batchSize);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("scanning dataset - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("scanning dataset - execution exception", ex);
        }
    }

    // Returns null if not found - upper layers do any throwing
    public FireStoreObject retrieveById(Firestore firestore, String collectionId, String objectId) {
        ApiFuture<FireStoreObject> transaction = firestore.runTransaction(xn -> {
            QueryDocumentSnapshot docSnap = lookupByObjectId(firestore, collectionId, objectId, xn);
            if (docSnap == null) {
                return null;
            }
            return docSnap.toObject(FireStoreObject.class);
        });

        return fireStoreUtils.transactionGet("retrieveById", transaction);
    }

    // Returns null if not found - upper layers do any throwing
    public FireStoreObject retrieveByPath(Firestore firestore, String collectionId, String fullPath) {
        ApiFuture<FireStoreObject> transaction = firestore.runTransaction(xn -> {
            DocumentSnapshot docSnap = lookupByObjectPath(firestore, collectionId, fullPath, xn);
            if (docSnap == null) {
                return null;
            }
            return docSnap.toObject(FireStoreObject.class);
        });

        return fireStoreUtils.transactionGet("retrieveByPath", transaction);

    }

/*
    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray) {
        List<String> missingIds = new ArrayList<>();
        for (String objectId : refIdArray) {
            if (!lookupByIdAndType(dataset, objectId, objectType)) {
                missingIds.add(objectId);
            }
        }
        return missingIds;
    }
*/
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
                lookupPath = fireStoreUtils.getDirectoryPath(lookupPath);
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

    List<FireStoreObject> enumerateDirectory(Firestore firestore, String collectionId, String dirPath) {
        ApiFuture<List<FireStoreObject>> transaction = firestore.runTransaction(xn -> {
            Query query = firestore.collection(collectionId).whereEqualTo("path", dirPath);
            ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            List<FireStoreObject> objectList =
                documents
                    .stream()
                    .map(document -> document.toObject(FireStoreObject.class))
                    .collect(Collectors.toList());
            return objectList;
        });

        return fireStoreUtils.transactionGet("enumerateDirectory", transaction);
    }

    // As mentioned at the top of the module, we can't use forward slash in a FireStore document
    // name, so we do this encoding.
    private static final char DOCNAME_SEPARATOR = '\u001c';
    private String encodePathAsFirestoreDocumentName(String path) {
        return StringUtils.replaceChars(path, '/', DOCNAME_SEPARATOR);
    }

    private DocumentReference getDocRef(Firestore firestore, String collectionId, FireStoreObject fireStoreObject) {
        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());
        String lookupPath = makeLookupPath(fullPath);
        return firestore.collection(collectionId).document(encodePathAsFirestoreDocumentName(lookupPath));
    }

    private DocumentSnapshot lookupByObjectPath(Firestore firestore,
                                                String collectionId,
                                                String lookupPath,
                                                Transaction xn) {
        try {
            DocumentReference docRef =
                firestore.collection(collectionId).document(encodePathAsFirestoreDocumentName(lookupPath));
            ApiFuture<DocumentSnapshot> docSnapFuture = xn.get(docRef);
            return docSnapFuture.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("lookup object path - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("lookup object path - execution exception", ex);
        }
    }

    // Returns null if not found
    private QueryDocumentSnapshot lookupByObjectId(Firestore firestore,
                                                   String collectionId,
                                                   String objectId,
                                                   Transaction xn) {
        try {
            CollectionReference datasetCollection = firestore.collection(collectionId);
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

    private FireStoreObject makeDirectoryObject(String dataset, String lookupDirPath) {
        // We have some special cases to deal with at the top of the directory tree.
        String fullPath = makePathFromLookupPath(lookupDirPath);
        String dirPath = fireStoreUtils.getDirectoryPath(fullPath);
        String objName = fireStoreUtils.getObjectName(fullPath);
        if (StringUtils.isEmpty(fullPath)) {
            // This is the root directory - it doesn't have a path or a name
            dirPath = StringUtils.EMPTY;
            objName = StringUtils.EMPTY;
        } else if (StringUtils.isEmpty(dirPath)) {
            // This is an object in the root directory - it needs to have the root path.
            dirPath = "/";
        }

        return new FireStoreObject()
            .objectId(UUID.randomUUID().toString())
            .fileRef(false)
            .path(dirPath)
            .name(objName)
            .fileCreatedDate(Instant.now().toString());
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
