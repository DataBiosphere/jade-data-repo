package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.StringListCompare;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.exception.FileAlreadyExistsException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.cloud.firestore.Firestore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ActiveProfiles({"google", "connectedtest"})
@Tag(Connected.TAG)
@EmbeddedDatabaseTest
class FireStoreDirectoryDaoTest {

  @Autowired private FireStoreDirectoryDao directoryDao;

  @Autowired private ConfigurationService configurationService;

  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;
  @Autowired private ConfigurationService configService;

  private String datasetId;
  private String collectionId;
  private Firestore firestore;

  @BeforeAll
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);

    // Create dataset so that we have a firestore instance to test with
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    var summaryModel = connectedOperations.createDataset(billingProfile, "dataset-minimal.json");

    datasetId = summaryModel.getId().toString();
    collectionId = "directoryDaoTest_" + datasetId;
    firestore = TestFirestoreProvider.getFirestore(summaryModel.getDataProject());
  }

  @AfterEach
  public void cleanupAfterEachTest() throws Exception {
    if (datasetId != null) {
      directoryDao.deleteDirectoryEntriesFromCollection(firestore, collectionId);
    }
  }

  @AfterAll
  public void cleanup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    connectedOperations.teardown();
  }

  @Test
  // Tests createFileRef, deleteDirectoryEntry, retrieveById, retrieveByPath
  void createDeleteTest() throws Exception {
    FireStoreDirectoryEntry fileA = makeFileObject("/adir/A");

    // Verify file A should not exist
    FireStoreDirectoryEntry testFileA =
        directoryDao.retrieveById(firestore, collectionId, fileA.getFileId());
    assertThat("Object id does not exist", testFileA, is(nullValue()));

    // Create the file
    directoryDao.createDirectoryEntry(firestore, collectionId, fileA);
    testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getFileId());
    assertThat("Object exists and is file object", testFileA.getIsFileRef());
    assertThat("Dataset id matches", datasetId, equalTo(testFileA.getDatasetId()));

    // Test overwrite semantics - a second create acts as an update
    String updatedDatasetId = datasetId + "X";
    fileA.datasetId(updatedDatasetId);
    directoryDao.createDirectoryEntry(firestore, collectionId, fileA);
    testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getFileId());
    assertThat("Object id exists", testFileA, is(notNullValue()));
    assertThat("Is file object", testFileA.getIsFileRef());
    assertThat("Updated id matches", testFileA.getDatasetId(), equalTo(updatedDatasetId));

    // Lookup the directory by path to get its object id so that we can make sure
    // it goes away when we delete the file.
    FireStoreDirectoryEntry dirA = directoryDao.retrieveByPath(firestore, collectionId, "/adir");
    assertThat("Directory exists", dirA, is(notNullValue()));
    assertThat("Is dir object", dirA.getIsFileRef(), is(false));

    // Delete file and verify everything is gone
    boolean objectExisted =
        directoryDao.deleteDirectoryEntry(firestore, collectionId, fileA.getFileId());
    assertThat("Object existed", objectExisted);
    testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getFileId());
    assertThat("File was deleted", testFileA, is(nullValue()));
    FireStoreDirectoryEntry testDirA =
        directoryDao.retrieveById(firestore, collectionId, dirA.getFileId());
    assertThat("Directory was deleted", testDirA, is(nullValue()));

    // Delete again. Should succeed and let us know the object didn't exist
    objectExisted = directoryDao.deleteDirectoryEntry(firestore, collectionId, fileA.getFileId());
    assertThat("Object did not exist", objectExisted, is(false));
  }

  @Test
  // Tests validateRefIds, enumerateDirectory, deleteDirectoryEntriesFromCollection, retrieveById,
  // retrieveByPath
  void directoryOperationsTest() throws Exception {
    List<FireStoreDirectoryEntry> fileObjects = new ArrayList<>();
    fileObjects.add(makeFileObject("/adir/A1"));
    fileObjects.add(makeFileObject("/adir/bdir/B1"));
    fileObjects.add(makeFileObject("/adir/bdir/cdir/C1"));
    fileObjects.add(makeFileObject("/adir/bdir/cdir/C2"));
    fileObjects.add(makeFileObject("/adir/bdir/B2"));
    fileObjects.add(makeFileObject("/adir/A2"));

    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : fileObjects) {
      directoryDao.createDirectoryEntry(firestore, collectionId, fireStoreDirectoryEntry);
    }

    // Test all valid file references
    List<String> fileRefs =
        fileObjects.stream().map(FireStoreDirectoryEntry::getFileId).collect(Collectors.toList());
    List<String> mismatches =
        directoryDao.validateRefIds(firestore, collectionId, fileRefs, "testCol");
    assertThat("No invalid file refs", mismatches.size(), equalTo(0));
    List<String> mismatches = directoryDao.validateRefIds(firestore, collectionId, fileRefs, "testCol");
    assertThat("No invalid file refs", mismatches, empty());

    List<String> badids = Arrays.asList("badid1", "badid2");

    // Test with invalid file references
    List<String> badFileRefs = new ArrayList<>();
    badFileRefs.addAll(fileRefs);
    badFileRefs.addAll(badids);

    mismatches = directoryDao.validateRefIds(firestore, collectionId, badFileRefs, "testCol");
    assertThat("Caught invalid file refs", mismatches.size(), equalTo(badids.size()));

    StringListCompare listCompare = new StringListCompare(mismatches, badids);
    assertThat("Bad ids match", listCompare.compare());

    // Test FireStoreBatchQueryIterator by making the batch size small
    TestUtils.setConfigParameterValue(
        configurationService, ConfigEnum.FIRESTORE_QUERY_BATCH_SIZE, "1", "setFirestoreBatch");
    // Test enumeration with adir. We should get three things back: two files (A1, A2) and a
    // directory (bdir).
    List<FireStoreDirectoryEntry> enumList =
        directoryDao.enumerateDirectory(firestore, collectionId, "/adir");
    assertThat("Correct number of object returned", enumList.size(), equalTo(3));
    List<String> expectedNames = Arrays.asList("A1", "A2", "bdir");
    List<String> enumNames = enumList.stream().map(FireStoreDirectoryEntry::getName).toList();

    StringListCompare enumCompare = new StringListCompare(expectedNames, enumNames);
    assertThat("Enum names match", enumCompare.compare());

    // Now add to the ref list all of the valid file object ids and then include the directory ids.
    // We'll use that
    // list to make sure that "delete everything" really deletes everything.
    fileRefs.add(retrieveDirectoryObjectId("/adir"));
    fileRefs.add(retrieveDirectoryObjectId("/adir/bdir"));
    fileRefs.add(retrieveDirectoryObjectId("/adir/bdir/cdir"));

    directoryDao.deleteDirectoryEntriesFromCollection(firestore, collectionId);

    for (String objectId : fileRefs) {
      FireStoreDirectoryEntry fso = directoryDao.retrieveById(firestore, collectionId, objectId);
      assertThat("File or dir object is deleted", fso, is(nullValue()));
    }
  }

  @Test
  // Tests that bulk filesystem ingest works  initially and when there are collisions with the same
  // load tag and failures when there are collisions with different load tags
  void bulkDirectoryEntryOperationsTest() throws Exception {
    List<FireStoreDirectoryEntry> initDirectories =
        List.of(
            makeFileObject("/adir/A1"),
            makeFileObject("/adir/bdir/B1"),
            makeFileObject("/adir/bdir/cdir"),
            makeFileObject("/adir/bdir/cdir/C2"),
            makeFileObject("/adir/bdir/B2"),
            makeFileObject("/adir/A2"));

    String initialLoadTag = "lt1";
    String nextLoadTag = "lt2";

    initDirectories.forEach(d -> d.loadTag(initialLoadTag));

    Map<UUID, UUID> initInsertResults =
        directoryDao.upsertDirectoryEntries(firestore, collectionId, initDirectories);
    assertThat(
        "the correct number were inserted (e.g. no conflicts)",
        initInsertResults.entrySet(),
        empty());

    // Insert a subset of objects (only the B3 directory should be new) using the same load tag
    List<FireStoreDirectoryEntry> nextDirectories =
        List.of(makeFileObject("/adir/A1"), makeFileObject("/adir/bdir/B3"));
    nextDirectories.forEach(d -> d.loadTag(initialLoadTag));

    Map<UUID, UUID> nextInsertResults =
        directoryDao.upsertDirectoryEntries(firestore, collectionId, nextDirectories);
    assertThat("the correct number were inserted", nextInsertResults.entrySet(), hasSize(1));

    // Inserts the same subset but with a different load tag.  This should throw
    nextDirectories.forEach(d -> d.loadTag(nextLoadTag));
    FileSystemExecutionException conflictingLoadTagsFail =
        assertThrows(
            "conflicting load tags fail",
            FileSystemExecutionException.class,
            () -> directoryDao.upsertDirectoryEntries(firestore, collectionId, nextDirectories));
    assertThat(
        "cause is correct",
        conflictingLoadTagsFail.getCause().getCause(),
        isA(FileAlreadyExistsException.class));
  }

  @Test
  // Tests that bulk directory ingest works  initially and when there are collisions with the same
  // load tag and failures when there are collisions with different load tags
  void bulkDirectoryOperationsFSObjectsTest() throws Exception {
    List<FireStoreDirectoryEntry> initialFileObjects =
        List.of(
            makeFileObject("/m/adir/A1/file"),
            makeFileObject("/m/adir/bdir/B1/file"),
            makeFileObject("/m/adir/bdir/cdir/C1/file"),
            makeFileObject("/m/adir/bdir/cdir/C2/file"),
            makeFileObject("/m/adir/bdir/B2/file"),
            makeFileObject("/m/adir/A2/file"));

    String initialLoadTag = "lt1";
    String secondLoadTag = "lt2";

    initialFileObjects.forEach(f -> f.loadTag(initialLoadTag));

    Map<UUID, UUID> initialConflicts =
        directoryDao.upsertDirectoryEntries(firestore, collectionId, initialFileObjects);
    assertThat("the correct number were inserted", initialConflicts.entrySet(), empty());

    // Insert a subset of objects (only the B3 directory should be new) using the same load tag
    List<FireStoreDirectoryEntry> nextFileObjects =
        List.of(makeFileObject("/m/adir/A1/file"), makeFileObject("/m/adir/bdir/B3/file"));
    nextFileObjects.forEach(f -> f.loadTag(initialLoadTag));

    Map<UUID, UUID> nextConflicts =
        directoryDao.upsertDirectoryEntries(firestore, collectionId, nextFileObjects);
    assertThat("the correct number were inserted", nextConflicts.entrySet(), hasSize(1));

    // Inserts the same subset but with a different load tag. This should throw
    nextFileObjects.forEach(f -> f.loadTag(secondLoadTag));
    FileSystemExecutionException conflictingLoadTagsFail =
        assertThrows(
            "conflicting load tags fail",
            FileSystemExecutionException.class,
            () -> directoryDao.upsertDirectoryEntries(firestore, collectionId, nextFileObjects));
    assertThat(
        "cause is correct",
        conflictingLoadTagsFail.getCause().getCause(),
        isA(FileAlreadyExistsException.class));
  }

  @Test
  void testEnumerateFileRefEntries() throws InterruptedException {
    List<FireStoreDirectoryEntry> noFiles =
        directoryDao.enumerateFileRefEntries(firestore, collectionId, 0, 10);
    assertThat(noFiles.size(), equalTo(0));

    List<FireStoreDirectoryEntry> fireStoreDirectoryEntries =
        List.of(
            makeFileObject("/adir/A1"),
            makeFileObject("/adir/bdir/B1"),
            makeFileObject("/adir/bdir/B2"),
            makeFileObject("/adir/bdir/cdir/C1"),
            makeFileObject("/adir/bdir/cdir/C2"));

    for (FireStoreDirectoryEntry fireStoreDirectoryEntry : fireStoreDirectoryEntries) {
      directoryDao.createDirectoryEntry(firestore, collectionId, fireStoreDirectoryEntry);
    }

    List<FireStoreDirectoryEntry> fileEntries =
        fireStoreDirectoryEntries.stream().filter(FireStoreDirectoryEntry::getIsFileRef).toList();

    List<FireStoreDirectoryEntry> allFiles =
        directoryDao.enumerateFileRefEntries(firestore, collectionId, 0, 10);
    assertThat(allFiles, equalTo(fileEntries));

    List<FireStoreDirectoryEntry> offsetFiles =
        directoryDao.enumerateFileRefEntries(firestore, collectionId, 1, 10);
    assertThat(offsetFiles, equalTo(fileEntries.subList(1, 5)));

    List<FireStoreDirectoryEntry> limitFiles =
        directoryDao.enumerateFileRefEntries(firestore, collectionId, 0, 2);
    assertThat(limitFiles, equalTo(fileEntries.subList(0, 2)));
  }

  private String retrieveDirectoryObjectId(String fullPath) throws InterruptedException {
    FireStoreDirectoryEntry entry = directoryDao.retrieveByPath(firestore, collectionId, fullPath);
    return entry.getFileId();
  }

  private FireStoreDirectoryEntry makeFileObject(String fullPath) {
    return new FireStoreDirectoryEntry()
        .fileId(UUID.randomUUID().toString())
        .isFileRef(true)
        .path(FileMetadataUtils.getDirectoryPath(fullPath))
        .name(FileMetadataUtils.getName(fullPath))
        .datasetId(datasetId);
  }
}
