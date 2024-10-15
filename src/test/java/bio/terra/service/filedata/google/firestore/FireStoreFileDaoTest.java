package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.filedata.exception.FileAlreadyExistsException;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.cloud.firestore.Firestore;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
class FireStoreFileDaoTest {
  private final Long FILE_SIZE = 42L;
  private final Long CHANGED_FILE_SIZE = 22L;

  @Autowired private FireStoreFileDao fileDao;
  @Autowired private FireStoreUtils fireStoreUtils;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private IamProviderInterface samService;
  private String datasetId;
  private Firestore firestore;
  private List<FireStoreDirectoryEntry> directoryEntries;
  private List<FireStoreFile> files;
  private List<String> objects;

  @BeforeAll
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);

    // Create dataset so that we have a firestore instance to test with
    BillingProfileModel billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    var summaryModel = connectedOperations.createDataset(billingProfile, "dataset-minimal.json");
    datasetId = summaryModel.getId().toString();

    firestore = TestFirestoreProvider.getFirestore(summaryModel.getDataProject());
  }

  @BeforeEach
  public void seedDirectoryEntries() throws InterruptedException {
    // Make sure there aren't any lingering entries from other tests - directory is empty
    fileDao.deleteFilesFromDataset(firestore, datasetId, fsf -> {});
    assertThat(
        "Cleanup from previous test successful",
        fileDao.enumerateAllWithEmptyField(firestore, datasetId, "fileId"),
        empty());
    directoryEntries = null;
    files = null;
    objects = null;

    // seed firestore with a collection of directory and file entries
    directoryEntries =
        IntStream.range(0, 5)
            .mapToObj(i -> new FireStoreDirectoryEntry().fileId(UUID.randomUUID().toString()))
            .toList();
    files =
        directoryEntries.stream()
            .map(FireStoreDirectoryEntry::getFileId)
            .map(this::makeFile)
            .toList();
    for (FireStoreFile file : files) {
      fileDao.upsertFileMetadata(firestore, datasetId, file);
    }
    objects = files.stream().map(FireStoreFile::getFileId).toList();
  }

  @AfterEach
  public void cleanupAfterEachTest() throws Exception {
    if (datasetId != null) {
      fileDao.deleteFilesFromDataset(firestore, datasetId, fsf -> {});
    }
  }

  @AfterAll
  public void cleanup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    connectedOperations.teardown();
  }

  @Test
  void testRetrieveFileMetadata() throws Exception {
    for (String obj : objects) {
      assertThat(
          "All files were added to firestore and that the correct file size is returned",
          fileDao.retrieveFileMetadata(firestore, datasetId, obj).getSize(),
          equalTo(FILE_SIZE));
    }

    // batch retrieve
    List<FireStoreFile> retrievedFiles =
        fileDao.batchRetrieveFileMetadata(firestore, datasetId, directoryEntries);
    assertEquals(files, retrievedFiles);
  }

  @Test
  void testRetrieveNonExistentFileMetadata() throws Exception {
    FireStoreDirectoryEntry directoryEntry =
        new FireStoreDirectoryEntry().fileId(UUID.randomUUID().toString());
    assertThrows(
        FileSystemCorruptException.class,
        () -> fileDao.batchRetrieveFileMetadata(firestore, datasetId, List.of(directoryEntry)));
  }

  @Test
  void testUpdateFileMetadata() throws Exception {
    // update file size field
    var file0 = files.get(0).size(CHANGED_FILE_SIZE);
    fileDao.upsertFileMetadata(firestore, datasetId, file0);
    assertThat(
        "Returned updated file size for file0",
        fileDao.retrieveFileMetadata(firestore, datasetId, file0.getFileId()).getSize(),
        equalTo(CHANGED_FILE_SIZE));
  }

  @Test
  void testEnumerateAllWithEmptyField() throws Exception {
    var file1 = files.get(1).checksumMd5("md5");
    fileDao.upsertFileMetadata(firestore, datasetId, file1);
    List<FireStoreFile> filesWithNullMd5Field =
        fileDao.enumerateAllWithEmptyField(firestore, datasetId, "checksumMd5");
    assertThat(
        "All but one file has empty field", filesWithNullMd5Field, hasSize(files.size() - 1));
  }

  @Test
  void testConflictingLoadTags() throws Exception {
    var file2 = files.get(2); // By default it should have loadTag = "loadTag"
    fileDao.upsertFileMetadata(firestore, datasetId, file2);
    Throwable cause =
        assertThrows(
                "upsert fails with load tag conflict (and try the list uploader)",
                FileSystemExecutionException.class,
                () ->
                    fileDao.upsertFileMetadata(firestore, datasetId, List.of(file2.loadTag("lt2"))))
            .getCause();
    assertThat(
        "Correct cause triggered the error",
        cause.getCause(),
        isA(FileAlreadyExistsException.class));
  }

  @Test
  void testDeleteSingleFileMetadata() throws Exception {
    var file0 = files.get(0);
    assertThat("File existed", fileDao.deleteFileMetadata(firestore, datasetId, file0.getFileId()));
    assertThat(
        "File entry was deleted",
        fileDao.retrieveFileMetadata(firestore, datasetId, file0.getFileId()),
        is(nullValue()));
  }

  @Test
  void testDeleteAllFileMetadataEntries() throws InterruptedException {
    List<String> deleteIds = new ArrayList<>();
    fileDao.deleteFilesFromDataset(
        firestore,
        datasetId,
        fsf -> {
          synchronized ((deleteIds)) {
            deleteIds.add(fsf.getFileId());
          }
        });
    assertThat(
        "Deleted id list matched created id list",
        deleteIds,
        containsInAnyOrder(objects.toArray(new String[0])));
    for (String fileId : objects) {
      FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
      assertThat("File entry was deleted", existCheck, is(nullValue()));
    }
  }

  private FireStoreFile makeFile(String fileId) {
    return new FireStoreFile()
        .fileId(fileId)
        .mimeType("application/test")
        .description("file")
        .bucketResourceId("BostonBucket")
        .gspath("gs://server.example.com/" + fileId)
        .loadTag("loadTag")
        .size(FILE_SIZE);
  }
}
