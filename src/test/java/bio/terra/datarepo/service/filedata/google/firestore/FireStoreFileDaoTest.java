package bio.terra.datarepo.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.datarepo.common.category.Connected;
import bio.terra.datarepo.common.fixtures.StringListCompare;
import bio.terra.datarepo.model.ConfigFaultCountedModel;
import bio.terra.datarepo.model.ConfigFaultModel;
import bio.terra.datarepo.model.ConfigGroupModel;
import bio.terra.datarepo.model.ConfigModel;
import bio.terra.datarepo.model.ConfigParameterModel;
import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import com.google.cloud.firestore.Firestore;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
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
public class FireStoreFileDaoTest {
  private final Logger logger =
      LoggerFactory.getLogger(
          "bio.terra.datarepo.service.filedata.google.firestore.FireStoreFileDaoTest");
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
    String objectId = file1.getFileId();

    FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    assertNull("Object id does not exists", existCheck);
    fileDao.createFileMetadata(firestore, datasetId, file1);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    assertNotNull("Object id exists", existCheck);
    assertThat("Correct size", existCheck.getSize(), equalTo(FILE_SIZE));

    file1.size(CHANGED_FILE_SIZE);
    fileDao.createFileMetadata(firestore, datasetId, file1);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    assertNotNull("Object id exists", existCheck);
    assertThat("Correct size", existCheck.getSize(), equalTo(CHANGED_FILE_SIZE));

    boolean fileExisted = fileDao.deleteFileMetadata(firestore, datasetId, objectId);
    assertTrue("File existed before delete", fileExisted);
    existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    assertNull("Object id does not exists", existCheck);

    fileExisted = fileDao.deleteFileMetadata(firestore, datasetId, objectId);
    assertFalse("File doesn't exist after delete", fileExisted);
  }

  @Test
  public void deleteAllFilesTest() throws Exception {
    // Make some files
    List<FireStoreFile> fileList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      FireStoreFile fsf = makeFile();
      fileDao.createFileMetadata(firestore, datasetId, fsf);
      fileList.add(fsf);
    }

    List<String> fileIds =
        fileList.stream().map(fsf -> fsf.getFileId()).collect(Collectors.toList());

    List<String> deleteIds = new ArrayList<>();

    // Delete the files; our function collects the deleted object ids in a list
    fileDao.deleteFilesFromDataset(firestore, datasetId, fsf -> deleteIds.add(fsf.getFileId()));

    StringListCompare listCompare = new StringListCompare(fileIds, deleteIds);
    assertTrue("Deleted id list matched created id list", listCompare.compare());

    for (String fileId : fileIds) {
      FireStoreFile existCheck = fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
      assertNull("File is deleted", existCheck);
    }
  }

  // Default settings of the thought should result in a retry failure
  @Test(expected = StatusRuntimeException.class)
  public void faultRetrieveRetryFail() throws Exception {
    configurationService.setFault(ConfigEnum.FIRESTORE_RETRIEVE_FAULT.name(), true);

    setConfigParameterValue(ConfigEnum.FIRESTORE_RETRIES, "1", "setRetryParameter");

    FireStoreFile file1 = makeFile();
    String objectId = file1.getFileId();

    fileDao.createFileMetadata(firestore, datasetId, file1);
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

    fileDao.createFileMetadata(firestore, datasetId, file1);
    fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
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

  private void setConfigParameterValue(ConfigEnum config, String newValue, String label) {
    ConfigModel retryConfigModel =
        new ConfigModel()
            .name(config.name())
            .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
            .parameter(new ConfigParameterModel().value(newValue));
    ConfigGroupModel group = new ConfigGroupModel().label(label).addGroupItem(retryConfigModel);
    configurationService.setConfig(group);
  }
}
