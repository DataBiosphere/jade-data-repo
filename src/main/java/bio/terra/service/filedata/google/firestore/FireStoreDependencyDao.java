package bio.terra.service.filedata.google.firestore;

import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_QUERY_BATCH_SIZE;
import static bio.terra.service.configuration.ConfigEnum.FIRESTORE_SNAPSHOT_BATCH_SIZE;

import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.resourcemanagement.ResourceService;
import com.google.api.client.util.Lists;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class FireStoreDependencyDao {
  private final Logger logger = LoggerFactory.getLogger(FireStoreDependencyDao.class);

  // The collection name has non-hexadecimal characters in it, so it won't collide with any dataset
  // id.
  private static final String DEPENDENCY_COLLECTION_NAME = "-dependencies";

  private final FireStoreUtils fireStoreUtils;
  private final ResourceService resourceService;
  private final ConfigurationService configurationService;

  @Autowired
  public FireStoreDependencyDao(
      FireStoreUtils fireStoreUtils,
      ResourceService resourceService,
      ConfigurationService configurationService) {
    this.fireStoreUtils = fireStoreUtils;
    this.resourceService = resourceService;
    this.configurationService = configurationService;
  }

  public boolean fileHasSnapshotReference(Dataset dataset, String fileId)
      throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());

    // check if firestore collection with file id has any documents
    CollectionReference depColl = firestore.collection(dependencyCollectionName);
    Query query = depColl.whereEqualTo("fileId", fileId).limit(1);
    return fireStoreUtils.collectionHasDocuments(firestore, query);
  }

  public boolean datasetHasSnapshotReference(Dataset dataset) throws InterruptedException {
    Firestore firestore =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId()).getFirestore();
    String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());

    // check to see if the datasets collection contains any dependencies
    Query collectionQuery = firestore.collection(dependencyCollectionName);
    return fireStoreUtils.collectionHasDocuments(firestore, collectionQuery);
  }

  public List<String> getDatasetSnapshotFileIds(Dataset dataset, String snapshotId)
      throws InterruptedException {
    FireStoreProject fireStoreProject =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
    String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
    Firestore firestore = fireStoreProject.getFirestore();
    CollectionReference depColl = firestore.collection(dependencyCollectionName);

    Query query = depColl.whereEqualTo("snapshotId", snapshotId);
    int batchSize = configurationService.getParameterValue(FIRESTORE_QUERY_BATCH_SIZE);
    FireStoreBatchQueryIterator queryIterator =
        new FireStoreBatchQueryIterator(query, batchSize, fireStoreUtils);

    List<String> fileIds = new ArrayList<>();
    for (List<QueryDocumentSnapshot> batch = queryIterator.getBatch();
        !CollectionUtils.isEmpty(batch);
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
    FireStoreProject fireStoreProject =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
    String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
    CollectionReference depColl =
        fireStoreProject.getFirestore().collection(dependencyCollectionName);

    List<List<String>> batches =
        ListUtils.partition(
            refIds, configurationService.getParameterValue(FIRESTORE_SNAPSHOT_BATCH_SIZE));

    for (List<String> batch : batches) {
      batchStoreSnapshotFileDependencies(depColl, snapshotId, batch);
    }
  }

  private void batchStoreSnapshotFileDependencies(
      CollectionReference depColl, String snapshotId, List<String> batch)
      throws InterruptedException {

    // Launch the lookups in parallel. Note Query.get() is returning an ApiFuture<QuerySnapshot>
    List<QuerySnapshot> querySnapshotList =
        fireStoreUtils.batchOperation(
            batch,
            fileId ->
                depColl
                    .whereEqualTo("fileId", fileId)
                    .whereEqualTo("snapshotId", snapshotId)
                    .get());

    // Combine the batch inputs with the results from the lookups to be used in the next batch
    // process
    int index = 0;
    final Map<String, QuerySnapshot> fileIdToQuerySnapshot = new HashMap<>();
    for (QuerySnapshot querySnapshot : querySnapshotList) {
      String fileId = batch.get(index);
      fileIdToQuerySnapshot.put(fileId, querySnapshot);
      index++;
    }

    // Collect the set results
    fireStoreUtils.batchOperation(
        Lists.newArrayList(fileIdToQuerySnapshot.entrySet()),
        e -> {
          final String fileId = e.getKey();
          final QuerySnapshot querySnapshot = e.getValue();
          List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
          switch (documents.size()) {
            case 0:
              {
                // no dependency yet. Let's make one
                FireStoreDependency fireStoreDependency =
                    new FireStoreDependency().snapshotId(snapshotId).fileId(fileId).refCount(1L);
                DocumentReference docRef = depColl.document();
                return docRef.set(fireStoreDependency);
              }

            case 1:
              {
                // existing dependency; increment the reference count
                QueryDocumentSnapshot docSnap = documents.get(0);
                FireStoreDependency fireStoreDependency =
                    docSnap.toObject(FireStoreDependency.class);
                fireStoreDependency.refCount(fireStoreDependency.getRefCount() + 1);
                return docSnap.getReference().set(fireStoreDependency);
              }

            default:
              throw new FileSystemCorruptException(
                  "Found more than one document for a file dependency - fileId: " + fileId);
          }
        });
  }

  public void deleteSnapshotFileDependencies(Dataset dataset, String snapshotId)
      throws InterruptedException {
    FireStoreProject fireStoreProject =
        FireStoreProject.get(dataset.getProjectResource().getGoogleProjectId());
    String dependencyCollectionName = getDatasetDependencyId(dataset.getId().toString());
    Firestore firestore = fireStoreProject.getFirestore();
    CollectionReference depColl = firestore.collection(dependencyCollectionName);

    Query query = depColl.whereEqualTo("snapshotId", snapshotId);
    int batchSize = configurationService.getParameterValue(FIRESTORE_QUERY_BATCH_SIZE);

    // TODO - Could we just use scan collections instead?
    FireStoreBatchQueryIterator queryIterator =
        new FireStoreBatchQueryIterator(query, batchSize, fireStoreUtils);

    for (List<QueryDocumentSnapshot> batch = queryIterator.getBatch();
        !CollectionUtils.isEmpty(batch);
        batch = queryIterator.getBatch()) {

      fireStoreUtils.batchOperation(
          batch,
          docSnap -> {
            logger.info("deleting: " + docSnap.getReference().getPath());
            return docSnap.getReference().delete();
          });
    }
  }

  private String getDatasetDependencyId(String datasetId) {
    return datasetId + DEPENDENCY_COLLECTION_NAME;
  }
}
