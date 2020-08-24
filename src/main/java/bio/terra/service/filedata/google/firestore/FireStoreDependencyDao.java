package bio.terra.service.filedata.google.firestore;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.DataLocationService;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import org.apache.commons.collections4.ListUtils;
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
    private DataLocationService dataLocationService;
    private final ApplicationConfiguration applicationConfiguration;

    @Autowired
    public FireStoreDependencyDao(FireStoreUtils fireStoreUtils,
                                  DataLocationService dataLocationService,
                                  ApplicationConfiguration applicationConfiguration) {
        this.fireStoreUtils = fireStoreUtils;
        this.dataLocationService = dataLocationService;
        this.applicationConfiguration = applicationConfiguration;
    }

    public boolean fileHasSnapshotReference(Dataset dataset, String fileId) throws InterruptedException {
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        Query query = depColl.whereEqualTo("fileId", fileId);
        return hasReference(query);
    }

    public boolean datasetHasSnapshotReference(Dataset dataset) {
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        // check to see if the datasets collection contains any dependencies
        boolean hasDependencies = depColl.listDocuments().iterator().hasNext();
        return hasDependencies;
    }

    private boolean hasReference(Query query) throws InterruptedException {
        ApiFuture<QuerySnapshot> querySnapshot = query.get();

        try {
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            return (documents.size() > 0);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("has reference - execution exception", ex);
        }
    }

    public List<String> getDatasetSnapshotFileIds(Dataset dataset, String snapshotId) throws InterruptedException {
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
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
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("get file ids - execution exception", ex);
        }
    }

    public void storeSnapshotFileDependencies(Dataset dataset, String snapshotId, List<String> refIds)
        throws InterruptedException {

        // We construct the snapshot file system without using transactions. We can get away with that,
        // because no one can access this snapshot during its creation.
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        List<List<String>> batches =
            ListUtils.partition(refIds, applicationConfiguration.getFirestoreSnapshotBatchSize());

        for (List<String> batch : batches) {
            batchStoreSnapshotFileDependencies(depColl, snapshotId, batch);
        }
    }

    private void batchStoreSnapshotFileDependencies(
        CollectionReference depColl, String snapshotId, List<String> batch)
        throws InterruptedException {

        List<ApiFuture<QuerySnapshot>> getFutures = new ArrayList<>();
        List<ApiFuture<WriteResult>> setFutures = new ArrayList<>();

        // Launch the lookups in parallel
        for (String fileId : batch) {
            Query query = depColl.whereEqualTo("fileId", fileId).whereEqualTo("snapshotId", snapshotId);
            getFutures.add(query.get());
        }

        try {
            // Scan the lookup results and launch the sets in parallel
            int index = 0;
            for (ApiFuture<QuerySnapshot> future : getFutures) {
                String fileId = batch.get(index);

                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                switch (documents.size()) {
                    case 0: {
                        // no dependency yet. Let's make one
                        FireStoreDependency fireStoreDependency = new FireStoreDependency()
                            .snapshotId(snapshotId)
                            .fileId(fileId)
                            .refCount(1L);
                        DocumentReference docRef = depColl.document();
                        setFutures.add(docRef.set(fireStoreDependency));
                        break;
                    }

                    case 1: {
                        // existing dependency; increment the reference count
                        QueryDocumentSnapshot docSnap = documents.get(0);
                        FireStoreDependency fireStoreDependency = docSnap.toObject(FireStoreDependency.class);
                        fireStoreDependency.refCount(fireStoreDependency.getRefCount() + 1);
                        setFutures.add(docSnap.getReference().set(fireStoreDependency));
                        break;
                    }

                    default:
                        throw new FileSystemCorruptException("Found more than one document for a file dependency");
                }
                index++;
            }

            // Collect the set results
            for (ApiFuture<WriteResult> future : setFutures) {
                future.get();
            }

        } catch (ExecutionException e) {
            throw new FileSystemExecutionException("batch retrieved by id failed", e);
        }
    }

    public void deleteSnapshotFileDependencies(Dataset dataset, String snapshotId) throws InterruptedException {
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
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
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("delete dependencies - execution exception", ex);
        }
    }

    public void removeSnapshotFileDependency(Dataset dataset, String snapshotId, String fileId)
        throws InterruptedException {

        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        FireStoreProject fireStoreProject = FireStoreProject.get(dataProject.getGoogleProjectId());
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
