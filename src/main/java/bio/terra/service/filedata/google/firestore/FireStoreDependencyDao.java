package bio.terra.service.filedata.google.firestore;

import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.dataset.Dataset;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class FireStoreDependencyDao {
    private final Logger logger = LoggerFactory.getLogger(FireStoreDependencyDao.class);

    // The collection name has non-hexadecimal characters in it, so it won't collide with any dataset id.
    private static final String DEPENDENCY_COLLECTION_NAME = "-dependencies";

    private FireStoreUtils fireStoreUtils;

    @Autowired
    public FireStoreDependencyDao(FireStoreUtils fireStoreUtils) {
        this.fireStoreUtils = fireStoreUtils;
    }

    public boolean fileHasSnapshotReference(Dataset dataset, String fileId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        Query query = depColl.whereEqualTo("fileId", fileId);
        return hasReference(query);
    }

    public boolean datasetHasSnapshotReference(Dataset dataset) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        // check to see if the datasets collection contains any dependencies
        boolean hasDependencies = depColl.listDocuments().iterator().hasNext();
        return hasDependencies;
    }

    private boolean hasReference(Query query) {
        ApiFuture<QuerySnapshot> querySnapshot = query.get();

        try {
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            return (documents.size() > 0);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("has reference - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("has reference - execution exception", ex);
        }
    }

    public List<String> getDatasetSnapshotFileIds(Dataset dataset, String snapshotId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        Query query = depColl.whereEqualTo("snapshotId", snapshotId);
        ApiFuture<QuerySnapshot> querySnapshot = query.get();
        List<String> fileIds = new ArrayList<>();

        try {
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            for (DocumentSnapshot docSnap : documents) {
                FireStoreDependency fireStoreDependency = docSnap.toObject(FireStoreDependency.class);
                fileIds.add(fireStoreDependency.getFileId());
            }
            return fileIds;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("get file ids - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("get file ids - execution exception", ex);
        }
    }

    public void storeSnapshotFileDependencies(Dataset dataset, String snapshotId, List<String> refIds) {
        // TODO: Right now storing and deleting (below) are not done in a single
        // transaction. That is possible, but more complicated. The assumption is that at a higher layer
        // we will eventually implement some concurrency control so that incompatible operations - like
        // hard deleting a file in a dataset and deleting a snapshot that uses that file - will not happen
        // at the same time. Even if this is in a transaction, it doesn't keep us from getting in trouble
        // in conflict cases like that. I believe we have them sprinkled all over the code. If I'm wrong
        // about this, let me know and I can revamp to do something like: make a map of all files needing
        // to be added or incremented. Then perform all adds or updates (or deletes).
        for (String fileId : refIds) {
            storeSnapshotFileDependency(dataset, snapshotId, fileId);
        }
    }

    public void deleteSnapshotFileDependencies(Dataset dataset, String snapshotId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        Query query = depColl.whereEqualTo("snapshotId", snapshotId);
        ApiFuture<QuerySnapshot> querySnapshot = query.get();

        try {
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            for (DocumentSnapshot docSnap : documents) {
                logger.info("deleting: " + docSnap.toString());
                docSnap.getReference().delete();
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("delete dependencies - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("delete dependencies - execution exception", ex);
        }
    }

    public void storeSnapshotFileDependency(Dataset dataset, String snapshotId, String fileId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            Query query = depColl.whereEqualTo("fileId", fileId)
                .whereEqualTo("snapshotId", snapshotId);
            ApiFuture<QuerySnapshot> querySnapshot = query.get();

            // There should be zero or one documents. Since we keep track of greater than one reference using the
            // reference count.
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            switch (documents.size()) {
                case 0: {
                    // no dependency yet. Let's make one
                    FireStoreDependency fireStoreDependency = new FireStoreDependency()
                        .snapshotId(snapshotId)
                        .fileId(fileId)
                        .refCount(1L);

                    DocumentReference docRef = depColl.document();
                    xn.set(docRef, fireStoreDependency);
                    break;
                }

                case 1: {
                    // existing dependency; increment the reference count
                    QueryDocumentSnapshot docSnap = documents.get(0);
                    FireStoreDependency fireStoreDependency = docSnap.toObject(FireStoreDependency.class);
                    fireStoreDependency.refCount(fireStoreDependency.getRefCount() + 1);
                    xn.set(docSnap.getReference(), fireStoreDependency);
                    break;
                }

                default:
                    throw new FileSystemCorruptException("Found more than one document for a file dependency");
            }
            return null;
        });

        fireStoreUtils.transactionGet("store dependency", transaction);
    }

    public void removeSnapshotFileDependency(Dataset dataset, String snapshotId, String fileId) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getDataProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        ApiFuture<Void> transaction = fireStoreProject.getFirestore().runTransaction(xn -> {
            Query query = depColl.whereEqualTo("fileId", fileId)
                .whereEqualTo("snapshotId", snapshotId);
            ApiFuture<QuerySnapshot> querySnapshot = query.get();

            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            if (documents.size() == 0) {
                // No file - nothing to delete
                return null;
            }
            if (documents.size() > 1) {
                throw new FileSystemCorruptException("Found more than one document for a file dependency");
            }

            QueryDocumentSnapshot docSnap = documents.get(0);
            FireStoreDependency fireStoreDependency = docSnap.toObject(FireStoreDependency.class);
            if (fireStoreDependency.getRefCount() <= 1) {
                xn.delete(docSnap.getReference());
            } else {
                fireStoreDependency.refCount(fireStoreDependency.getRefCount() - 1);
                xn.set(docSnap.getReference(), fireStoreDependency);
            }
            return null;
        });

        fireStoreUtils.transactionGet("delete dependency", transaction);
    }

    private String getDatasetDependencyId(String datasetId) {
        return datasetId + DEPENDENCY_COLLECTION_NAME;
    }
}
