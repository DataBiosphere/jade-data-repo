package bio.terra.service.filedata.google.firestore;

import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.ResourceService;
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

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_QUERY_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;

@Component
public class FireStoreDependencyDao {
    private final Logger logger = LoggerFactory.getLogger(FireStoreDependencyDao.class);

    // The collection name has non-hexadecimal characters in it, so it won't collide with any dataset id.
    private static final String DEPENDENCY_COLLECTION_NAME = "-dependencies";

    private final FireStoreUtils fireStoreUtils;
    private final ResourceService resourceService;
    private final ConfigurationService configurationService;

    @Autowired
    public FireStoreDependencyDao(FireStoreUtils fireStoreUtils,
                                  ResourceService resourceService,
                                  ConfigurationService configurationService) {
        this.fireStoreUtils = fireStoreUtils;
        this.resourceService = resourceService;
        this.configurationService = configurationService;
    }

    public boolean fileHasSnapshotReference(Dataset dataset, String fileId) throws InterruptedException {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        Query query = depColl.whereEqualTo("fileId", fileId).limit(1);
        ApiFuture<QuerySnapshot> querySnapshot = query.get();

        try {
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
            return (documents.size() > 0);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("has reference - execution exception", ex);
        }
    }

    public boolean datasetHasSnapshotReference(Dataset dataset) {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);
        // check to see if the datasets collection contains any dependencies
        boolean hasDependencies = depColl.listDocuments().iterator().hasNext();
        return hasDependencies;
    }

    public List<String> getDatasetSnapshotFileIds(Dataset dataset, String snapshotId) throws InterruptedException {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        Query query = depColl.whereEqualTo("snapshotId", snapshotId);
        int batchSize = configurationService.getParameterValue(FIRESTORE_QUERY_BATCH_SIZE);
        FireStoreBatchQueryIterator queryIterator = new FireStoreBatchQueryIterator(query, batchSize);

        List<String> fileIds = new ArrayList<>();
        for (List<QueryDocumentSnapshot> batch = queryIterator.getBatch();
             batch != null;
             batch = queryIterator.getBatch()) {

            for (DocumentSnapshot docSnap : batch) {
                FireStoreDependency fireStoreDependency = docSnap.toObject(FireStoreDependency.class);
                fileIds.add(fireStoreDependency.getFileId());
            }
        }

        return fileIds;
    }

    public void storeSnapshotFileDependencies(Dataset dataset, String snapshotId, List<String> refIds)
        throws InterruptedException {

        // We construct the snapshot file system without using transactions. We can get away with that,
        // because no one can access this snapshot during its creation.
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        List<List<String>> batches =
            ListUtils.partition(refIds, configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE));

        for (List<String> batch : batches) {
            batchStoreSnapshotFileDependencies(depColl, snapshotId, batch);
        }
    }

    private void batchStoreSnapshotFileDependencies(
        CollectionReference depColl, String snapshotId, List<String> batch)
        throws InterruptedException {

        // Launch the lookups in parallel. Note Query.get() is returning an ApiFuture<QuerySnapshot>
        List<QuerySnapshot> querySnapshotList = fireStoreUtils.batchOperation(
            batch,
            fileId ->
                depColl.whereEqualTo("fileId", fileId)
                    .whereEqualTo("snapshotId", snapshotId).get()
            );

        // Scan the lookup results and launch the sets in parallel
        int index = 0;
        List<ApiFuture<WriteResult>> setFutures = new ArrayList<>();

        for (QuerySnapshot querySnapshot : querySnapshotList) {
            String fileId = batch.get(index);

            List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
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
                    throw new FileSystemCorruptException(
                        "Found more than one document for a file dependency - fileId: " + fileId);
            }
            index++;
        }

        // Collect the set results
        fireStoreUtils.batchOperation(
            setFutures,
            future -> future
        );
    }

    public void deleteSnapshotFileDependencies(Dataset dataset, String snapshotId) throws InterruptedException {
        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
        String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
        CollectionReference depColl = fireStoreProject.getFirestore().collection(dependencyCollectionName);

        Query query = depColl.whereEqualTo("snapshotId", snapshotId);
        int batchSize = configurationService.getParameterValue(FIRESTORE_QUERY_BATCH_SIZE);
        FireStoreBatchQueryIterator queryIterator = new FireStoreBatchQueryIterator(query, batchSize);

        for (List<QueryDocumentSnapshot> batch = queryIterator.getBatch();
             batch != null;
             batch = queryIterator.getBatch()) {

            fireStoreUtils.batchOperation(
                batch,
                docSnap -> {
                    logger.info("deleting: " + docSnap.getReference().getPath());
                    return docSnap.getReference().delete();
                }
            );
        }
    }

    public void removeSnapshotFileDependency(Dataset dataset, String snapshotId, String fileId)
        throws InterruptedException {

        FireStoreProject fireStoreProject = FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
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
                throw new FileSystemCorruptException(
                    "Found more than one document for a file dependency - fileId: " + fileId);
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
