package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.SnapshotCompute;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import com.google.cloud.firestore.Firestore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class FireStoreDaoTest {
  private final Logger logger = LoggerFactory.getLogger(FireStoreDaoTest.class);

  @Autowired private FireStoreDirectoryDao directoryDao;

  @Autowired private FireStoreFileDao fileDao;

  @Autowired private FireStoreDao dao;

  @Autowired private FireStoreUtils fireStoreUtils;

  @Autowired private FireStoreDependencyDao fireStoreDependencyDao;

  private Firestore firestore;
  private String pretendDatasetId;
  private String collectionId;
  private String snapshotId;

  @Before
  public void setup() throws Exception {
    firestore = TestFirestoreProvider.getFirestore();
    pretendDatasetId = UUID.randomUUID().toString();
    collectionId = "fsdaoDset_" + pretendDatasetId;
    snapshotId = "fsdaoSnap_" + pretendDatasetId;
  }

  @After
  public void cleanup() throws Exception {
    directoryDao.deleteDirectoryEntriesFromCollection(firestore, snapshotId);
    directoryDao.deleteDirectoryEntriesFromCollection(firestore, collectionId);
    fileDao.deleteFilesFromDataset(firestore, collectionId, i -> {});
  }

  // Test for snapshot file system
  // collectionId is the datasetId
  // snapshotId is obvious
  // - create dataset file system
  // - create subset snapshot file system
  // - do the compute and validate
  // Use binary for the sizes so each size combo will be unique
  @Test
  @SuppressFBWarnings(value = "DMI_HARDCODED_ABSOLUTE_FILENAME")
  public void snapshotTest() throws Exception {
    String collectionId = "fsdaoDset_" + pretendDatasetId;
    String snapshotId = "fsdaoSnap_" + pretendDatasetId;
    GoogleProjectResource projectResource =
        new GoogleProjectResource().googleProjectId(System.getenv("GOOGLE_CLOUD_DATA_PROJECT"));
    Dataset dataset =
        new Dataset().id(UUID.fromString(pretendDatasetId)).projectResource(projectResource);

    // Make files that will be in the snapshot
    List<FireStoreDirectoryEntry> snapObjects = new ArrayList<>();
    snapObjects.add(makeFileObject(collectionId, "/adir/A1", 1));
    snapObjects.add(makeFileObject(collectionId, "/adir/bdir/B1", 2));
    snapObjects.add(makeFileObject(collectionId, "/adir/bdir/cdir/C1", 4));
    snapObjects.add(makeFileObject(collectionId, "/adir/bdir/cdir/C2", 8));

    // And some files that won't be in the snapshot
    List<FireStoreDirectoryEntry> dsetObjects = new ArrayList<>();
    dsetObjects.add(makeFileObject(collectionId, "/adir/bdir/B2", 16));
    dsetObjects.add(makeFileObject(collectionId, "/adir/A2", 32));

    // Make the dataset file system
    List<FireStoreDirectoryEntry> fileObjects = new ArrayList<>(snapObjects);
    fileObjects.addAll(dsetObjects);
    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : fileObjects) {
      directoryDao.createDirectoryEntry(firestore, collectionId, fireStoreDirectoryEntry);
    }

    // Make the snapshot file system
    List<String> fileIdList = new ArrayList<>();
    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : snapObjects) {
      fileIdList.add(fireStoreDirectoryEntry.getFileId());
    }
    directoryDao.addEntriesToSnapshot(
        firestore, collectionId, "dataset", firestore, snapshotId, fileIdList, false);

    // Validate we can lookup files in the snapshot
    for (FireStoreDirectoryEntry dsetObject : snapObjects) {
      FireStoreDirectoryEntry snapObject =
          directoryDao.retrieveById(firestore, snapshotId, dsetObject.getFileId());
      assertNotNull("object found in snapshot", snapObject);
      assertThat("objectId matches", snapObject.getFileId(), equalTo(dsetObject.getFileId()));
      assertThat("path does not match", snapObject.getPath(), not(equalTo(dsetObject.getPath())));
    }

    // ------ test FireStoreDependencyDao ----
    // Before setting up the dependency file system, assert datasetHasSnapshotReference returns
    // false
    boolean noDependencies = fireStoreDependencyDao.datasetHasSnapshotReference(dataset);
    assertFalse("Dataset should not yet have dependencies", noDependencies);

    // Create dependency file system
    fireStoreDependencyDao.storeSnapshotFileDependencies(dataset, snapshotId, fileIdList);

    // Snapshot and File Dependency should now exist for dataset
    boolean hasReference = fireStoreDependencyDao.datasetHasSnapshotReference(dataset);
    assertTrue("Dataset should have dependencies", hasReference);

    boolean hasFileReference =
        fireStoreDependencyDao.fileHasSnapshotReference(dataset, snapObjects.get(0).getFileId());
    assertTrue("File should be referenced in snapshot", hasFileReference);

    // Validate dataset files do not have references
    boolean noFileReference =
        fireStoreDependencyDao.fileHasSnapshotReference(dataset, dsetObjects.get(0).getFileId());
    assertFalse("No dependency on files not referenced in snapshot", noFileReference);

    // Validate we cannot lookup dataset files in the snapshot
    for (FireStoreDirectoryEntry dsetObject : dsetObjects) {
      FireStoreDirectoryEntry snapObject =
          directoryDao.retrieveById(firestore, snapshotId, dsetObject.getFileId());
      assertNull("object not found in snapshot", snapObject);
    }

    // Compute the size and checksums
    FireStoreDirectoryEntry topDir = directoryDao.retrieveByPath(firestore, snapshotId, "/");
    List<FireStoreDirectoryEntry> updateBatch = new ArrayList<>();
    FireStoreDao.FirestoreComputeHelper helper = dao.getHelper(firestore, firestore, snapshotId);
    SnapshotCompute.computeDirectory(helper, topDir, updateBatch);
    directoryDao.batchStoreDirectoryEntry(firestore, snapshotId, updateBatch);

    // Check the accumulated size on the root dir
    FireStoreDirectoryEntry snapObject = directoryDao.retrieveByPath(firestore, snapshotId, "/");
    assertNotNull("root exists", snapObject);
    assertThat("Total size is correct", snapObject.getSize(), equalTo(15L));
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

    fileDao.createFileMetadata(firestore, datasetId, newFile);

    return new FireStoreDirectoryEntry()
        .fileId(fileId)
        .isFileRef(true)
        .path(FileMetadataUtils.getDirectoryPath(fullPath))
        .name(FileMetadataUtils.getName(fullPath))
        .datasetId(collectionId)
        .size(size)
        .checksumCrc32c(SnapshotCompute.computeCrc32c(fullPath))
        .checksumMd5(SnapshotCompute.computeMd5(fullPath));
  }
}
