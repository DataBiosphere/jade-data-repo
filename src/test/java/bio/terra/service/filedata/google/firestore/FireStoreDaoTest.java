package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.SnapshotCompute;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import com.google.cloud.firestore.Firestore;
import com.google.common.collect.Streams;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Tag(Connected.TAG)
@EmbeddedDatabaseTest
class FireStoreDaoTest {
  @Autowired private FireStoreDirectoryDao directoryDao;
  @Autowired private FireStoreFileDao fileDao;
  @Autowired private FireStoreDao dao;
  @Autowired private FireStoreUtils fireStoreUtils;
  @Autowired private FireStoreDependencyDao fireStoreDependencyDao;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;
  @Autowired private ConfigurationService configService;

  private Firestore firestore;
  private String datasetId;
  private String snapshotId;
  private Dataset dataset;
  private Snapshot snapshot;

  @BeforeEach
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();

    // Create dataset so that we have a firestore instance to test with
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    DatasetSummaryModel summaryModel =
        connectedOperations.createDataset(billingProfile, "dataset-minimal.json");
    GoogleProjectResource projectResource =
        new GoogleProjectResource().googleProjectId(summaryModel.getDataProject());
    dataset = new Dataset().id(summaryModel.getId()).projectResource(projectResource);
    datasetId = summaryModel.getId().toString();
    var snapshotIdUUID = UUID.randomUUID();
    snapshotId = snapshotIdUUID.toString();
    snapshot = new Snapshot().id(snapshotIdUUID).projectResource(projectResource);

    // real case will have separate dataset and snapshot instances
    // But, we can share this firestore instance for this test
    firestore = TestFirestoreProvider.getFirestore(summaryModel.getDataProject());
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (datasetId != null) {
      directoryDao.deleteDirectoryEntriesFromCollection(firestore, datasetId);
      directoryDao.deleteDirectoryEntriesFromCollection(firestore, snapshotId);
      fireStoreDependencyDao.deleteSnapshotFileDependencies(dataset, snapshotId);
      fileDao.deleteFilesFromDataset(firestore, datasetId, f -> {});
    }
    connectedOperations.teardown();
  }

  // Test for snapshot file system
  // collectionId is the datasetId
  // snapshotId is obvious
  // - create dataset file system
  // - create subset snapshot file system
  // - do the compute and validate
  // Use binary for the sizes so each size combo will be unique
  @Test
  void snapshotTest() throws Exception {

    // Make files that will be in the snapshot
    List<FireStoreDirectoryEntry> snapObjects = new ArrayList<>();
    snapObjects.add(makeFileObject(datasetId, "/adir/A1", 1));
    snapObjects.add(makeFileObject(datasetId, "/adir/bdir/B1", 2));
    snapObjects.add(makeFileObject(datasetId, "/adir/bdir/cdir/C1", 4));
    snapObjects.add(makeFileObject(datasetId, "/adir/bdir/cdir/C2", 8));

    // And some files that won't be in the snapshot
    List<FireStoreDirectoryEntry> dsetObjects = new ArrayList<>();
    dsetObjects.add(makeFileObject(datasetId, "/adir/bdir/B2", 16));
    dsetObjects.add(makeFileObject(datasetId, "/adir/A2", 32));

    List<String> dsfileIdList =
        Streams.concat(
                dsetObjects.stream().map(FireStoreDirectoryEntry::getFileId),
                snapObjects.stream().map(FireStoreDirectoryEntry::getFileId))
            .toList();

    // Make the dataset file system
    List<FireStoreDirectoryEntry> fileObjects = new ArrayList<>(snapObjects);
    fileObjects.addAll(dsetObjects);
    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : fileObjects) {
      directoryDao.createDirectoryEntry(firestore, datasetId, fireStoreDirectoryEntry);
    }

    // Make the snapshot file system
    List<String> snapfileIdList =
        snapObjects.stream().map(FireStoreDirectoryEntry::getFileId).toList();
    directoryDao.addEntriesToSnapshot(
        firestore, datasetId, "dataset", firestore, snapshotId, snapfileIdList, false);

    // Validate we can lookup files in the snapshot
    for (FireStoreDirectoryEntry dsetObject : snapObjects) {
      FireStoreDirectoryEntry snapObject =
          directoryDao.retrieveById(firestore, snapshotId, dsetObject.getFileId());
      assertThat("objectId matches", snapObject.getFileId(), equalTo(dsetObject.getFileId()));
      assertThat("path does not match", snapObject.getPath(), not(equalTo(dsetObject.getPath())));
    }

    // ------ test FireStoreDependencyDao ----
    // Before setting up the dependency file system, assert datasetHasSnapshotReference returns
    // false
    boolean noDependencies = fireStoreDependencyDao.datasetHasSnapshotReference(dataset);
    assertThat("Dataset should not yet have dependencies", noDependencies, is(false));

    // Create dependency file system
    fireStoreDependencyDao.storeSnapshotFileDependencies(dataset, snapshotId, snapfileIdList);

    // Snapshot and File Dependency should now exist for dataset
    boolean hasReference = fireStoreDependencyDao.datasetHasSnapshotReference(dataset);
    assertThat("Dataset should have dependencies", hasReference);

    boolean hasFileReference =
        fireStoreDependencyDao.fileHasSnapshotReference(dataset, snapObjects.get(0).getFileId());
    assertThat("File should be referenced in snapshot", hasFileReference);

    // Validate dataset files do not have references
    boolean noFileReference =
        fireStoreDependencyDao.fileHasSnapshotReference(dataset, dsetObjects.get(0).getFileId());
    assertThat("No dependency on files not referenced in snapshot", noFileReference, is(false));

    // Validate we cannot lookup dataset files in the snapshot
    for (FireStoreDirectoryEntry dsetObject : dsetObjects) {
      FireStoreDirectoryEntry snapObject =
          directoryDao.retrieveById(firestore, snapshotId, dsetObject.getFileId());
      assertThat("object not found in snapshot", snapObject, is(nullValue()));
    }

    // Compute the size and checksums
    FireStoreDirectoryEntry topDir = directoryDao.retrieveByPath(firestore, snapshotId, "/");
    List<FireStoreDirectoryEntry> updateBatch = new ArrayList<>();
    FireStoreDao.FirestoreComputeHelper helper = dao.getHelper(firestore, firestore, snapshotId);
    SnapshotCompute.computeDirectory(helper, topDir, updateBatch);
    directoryDao.batchStoreDirectoryEntry(firestore, snapshotId, updateBatch);

    // Check the accumulated size on the root dir
    FireStoreDirectoryEntry snapObject = directoryDao.retrieveByPath(firestore, snapshotId, "/");
    assertThat("Total size is correct", snapObject.getSize(), equalTo(15L));

    // Check that we can retrieve all with or without directories
    assertThat(
        "all dataset files and directories can be returned",
        dao.retrieveAllFileIds(dataset, true),
        hasSize(10));
    assertThat(
        "all dataset files (only) can be returned",
        dao.retrieveAllFileIds(dataset, false).stream().sorted().toList(),
        equalTo(dsfileIdList.stream().sorted().toList()));
    assertThat(
        "all snapshot files and directories can be returned",
        dao.retrieveAllFileIds(snapshot, true),
        hasSize(9));
    assertThat(
        "all snapshot files (only) can be returned",
        dao.retrieveAllFileIds(snapshot, false).stream().sorted().toList(),
        equalTo(snapfileIdList.stream().sorted().toList()));
  }

  private FireStoreDirectoryEntry makeFileObject(String datasetId, String fullPath, long size)
      throws InterruptedException {

    String fileId = UUID.randomUUID().toString();

    FireStoreFile newFile =
        new FireStoreFile()
            .fileId(fileId)
            .mimeType("application/test")
            .description("test")
            .bucketResourceId("test")
            .fileCreatedDate(Instant.now().toString())
            .gspath("gs://" + datasetId + "/" + fileId)
            .checksumCrc32c(SnapshotCompute.computeCrc32c(fullPath))
            .checksumMd5(SnapshotCompute.computeMd5(fullPath))
            .userSpecifiedMd5(false)
            .size(size);

    fileDao.upsertFileMetadata(firestore, datasetId, newFile);

    return new FireStoreDirectoryEntry()
        .fileId(fileId)
        .isFileRef(true)
        .path(FileMetadataUtils.getDirectoryPath(fullPath))
        .name(FileMetadataUtils.getName(fullPath))
        .datasetId(datasetId)
        .size(size)
        .checksumCrc32c(SnapshotCompute.computeCrc32c(fullPath))
        .checksumMd5(SnapshotCompute.computeMd5(fullPath));
  }
}
