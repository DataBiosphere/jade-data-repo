package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileAlreadyExistsException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.cloud.firestore.Firestore;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
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
public class FireStoreFileDaoTest {
  private final Logger logger = LoggerFactory.getLogger(FireStoreFileDaoTest.class);
  private final Long FILE_SIZE = 42L;
  private final Long CHANGED_FILE_SIZE = 22L;

  @Autowired private FireStoreFileDao fileDao;

  @Autowired private FireStoreUtils fireStoreUtils;

  @Autowired private ConfigurationService configurationService;

  private String datasetId;
  private Firestore firestore;

  @Before
  public void setup() throws Exception {
    configurationService.reset();
    datasetId = UUID.randomUUID().toString();
    firestore = TestFirestoreProvider.getFirestore();
  }

  @After
  public void cleanup() {
    configurationService.reset();
  }

  @Test
  public void createDeleteFileTest() throws Exception {
    FireStoreFile file1 = makeFile();
    FireStoreFile file2 = makeFile();
    String objectId1 = file1.getFileId();

    FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId1);
    assertNull("Object id does not exists", existCheck);
    fileDao.upsertFileMetadata(firestore, datasetId, file1);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId1);
    assertNotNull("Object id exists", existCheck);
    assertThat("Correct size", existCheck.getSize(), equalTo(FILE_SIZE));

    file1.size(CHANGED_FILE_SIZE);
    fileDao.upsertFileMetadata(firestore, datasetId, file1);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId1);
    assertNotNull("Object id exists", existCheck);
    assertThat("Correct size", existCheck.getSize(), equalTo(CHANGED_FILE_SIZE));

    file2.checksumMd5("foo");
    fileDao.upsertFileMetadata(firestore, datasetId, file2);
    List<FireStoreFile> filesWithNullMd5Field =
        fileDao.enumerateAllWithEmptyField(firestore, datasetId, "checksumMd5");
    assertThat("only one file has no md5", filesWithNullMd5Field, hasSize(1));
    assertThat(
        "file1's id has a null md5", filesWithNullMd5Field.get(0).getFileId(), equalTo(objectId1));

    boolean fileExisted = fileDao.deleteFileMetadata(firestore, datasetId, objectId1);
    assertTrue("File existed before delete", fileExisted);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId1);
    assertNull("Object id does not exists", existCheck);

    fileExisted = fileDao.deleteFileMetadata(firestore, datasetId, objectId1);
    assertFalse("File doesn't exist after delete", fileExisted);
  }

  @Test
  public void deleteAllFilesTest() throws Exception {
    // Make some files
    List<FireStoreFile> fileList = IntStream.range(0, 5).boxed().map(i -> makeFile()).toList();
    fileDao.upsertFileMetadata(firestore, datasetId, fileList);

    List<String> fileIds = fileList.stream().map(FireStoreFile::getFileId).toList();

    for (String fileId : fileIds) {
      FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
      assertNotNull("File entry was created", existCheck);
    }

    List<String> deleteIds = new ArrayList<>();

    // Delete the files; our function collects the deleted object ids in a list
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
        containsInAnyOrder(fileIds.toArray(new String[0])));

    for (String fileId : fileIds) {
      FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
      assertNull("File entry was deleted", existCheck);
    }
  }

  // Default settings of the thought should result in a retry failure
  @Test(expected = StatusRuntimeException.class)
  public void faultRetrieveRetryFail() throws Exception {
    configurationService.setFault(ConfigEnum.FIRESTORE_RETRIEVE_FAULT.name(), true);

    TestUtils.setConfigParameterValue(
        configurationService, ConfigEnum.FIRESTORE_RETRIES, "1", "setRetryParameter");

    FireStoreFile file1 = makeFile();
    String objectId = file1.getFileId();

    fileDao.upsertFileMetadata(firestore, datasetId, file1);
    fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
  }

  @Test()
  public void faultRetrieveRetrySuccess() throws Exception {
    ConfigFaultCountedModel countedModel =
        new ConfigFaultCountedModel()
            .skipFor(0)
            .insert(3)
            .rate(100)
            .rateStyle(ConfigFaultCountedModel.RateStyleEnum.FIXED);

    ConfigFaultModel configFaultModel =
        new ConfigFaultModel()
            .enabled(true)
            .faultType(ConfigFaultModel.FaultTypeEnum.COUNTED)
            .counted(countedModel);

    ConfigModel configModel =
        new ConfigModel()
            .name(ConfigEnum.FIRESTORE_RETRIEVE_FAULT.name())
            .configType(ConfigModel.ConfigTypeEnum.FAULT)
            .fault(configFaultModel);

    ConfigGroupModel configGroupModel =
        new ConfigGroupModel().label("faultRetrieveRetrySuccess").addGroupItem(configModel);
    configurationService.setConfig(configGroupModel);

    FireStoreFile file1 = makeFile();
    String objectId = file1.getFileId();

    fileDao.upsertFileMetadata(firestore, datasetId, file1);
    fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
  }

  @Test
  public void createFileWithConflictingLoadTagsTest() throws Exception {
    FireStoreFile file1 = makeFile();
    String objectId1 = file1.getFileId();

    FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId1);
    assertNull("Object id does not exists", existCheck);
    fileDao.upsertFileMetadata(firestore, datasetId, file1.loadTag("lt1"));
    Throwable cause =
        assertThrows(
                "upsert fails with load tag conflict (and try the list uploader)",
                FileSystemExecutionException.class,
                () ->
                    fileDao.upsertFileMetadata(firestore, datasetId, List.of(file1.loadTag("lt2"))))
            .getCause();
    assertThat(
        "Correct cause triggered the error",
        cause.getCause(),
        isA(FileAlreadyExistsException.class));
  }

  private FireStoreFile makeFile() {
    String fileId = UUID.randomUUID().toString();
    return new FireStoreFile()
        .fileId(fileId)
        .mimeType("application/test")
        .description("file")
        .bucketResourceId("BostonBucket")
        .gspath("gs://server.example.com/" + fileId)
        .size(FILE_SIZE);
  }
}
