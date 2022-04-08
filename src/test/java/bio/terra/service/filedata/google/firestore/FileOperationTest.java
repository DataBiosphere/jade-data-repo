package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetDaoUtils;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class FileOperationTest {

  @Autowired private MockMvc mvc;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConfigurationService configService;
  @Autowired private DatasetDao datasetDao;

  @MockBean private IamProviderInterface samService;

  @SpyBean private GoogleProjectService googleProjectService;

  private static Logger logger = LoggerFactory.getLogger(FileOperationTest.class);
  private int validFileCounter;
  private String coreBillingAccountId;
  private BillingProfileModel profileModel;
  private DatasetSummaryModel datasetSummary;
  private DatasetDaoUtils datasetDaoUtils;
  private ResourceService resourceService;

  @Before
  public void setup() throws Exception {
    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);

    // File generator indices
    validFileCounter = 0;

    // Retrieve billing info
    coreBillingAccountId = testConfig.getGoogleBillingAccountId();
    profileModel = connectedOperations.createProfileForAccount(coreBillingAccountId);

    datasetSummary = connectedOperations.createDataset(profileModel, "snapshot-test-dataset.json");
    // Make sure we start from a known configuration
    configService.reset();
    // Set the configuration so it is constant for the load tests
    ConfigModel concurrentConfig = configService.getConfig(ConfigEnum.LOAD_CONCURRENT_FILES.name());
    concurrentConfig.setParameter(new ConfigParameterModel().value("6"));
    ConfigModel driverWaitConfig =
        configService.getConfig(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS.name());
    driverWaitConfig.setParameter(new ConfigParameterModel().value("30"));
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel()
            .label("FileOperationTest")
            .addGroupItem(concurrentConfig)
            .addGroupItem(driverWaitConfig);
    configService.setConfig(configGroupModel);
    logger.info("--------begin test---------");
  }

  @After
  public void teardown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  private static String testDescription = "test file description";
  private static String testMimeType = "application/pdf";
  private static String testPdfFile = "File Design Notes.pdf";

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void fileOperationsTest() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    FileModel fileModel =
        connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
    assertThat("file path matches", fileModel.getPath(), equalTo(fileLoadModel.getTargetPath()));

    // Change the data location selector, verify that we can still delete the file
    // NOTE: the suppressed SpotBugs complaint is from the doReturn. It decides that no one
    // uses the bucketForFile call.
    String newBucketName = UUID.randomUUID().toString();
    doReturn(newBucketName).when(googleProjectService).bucketForFile(any());
    connectedOperations.deleteTestFile(datasetSummary.getId(), fileModel.getFileId());
    fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
    assertThat(
        "file path reflects new bucket location",
        fileModel.getFileDetail().getAccessUrl(),
        containsString(newBucketName));
    // Track the bucket so connected ops can remove it on teardown
    connectedOperations.addBucket(newBucketName);

    // lookup the file we just created
    String url =
        "/api/repository/v1/datasets/" + datasetSummary.getId() + "/files/" + fileModel.getFileId();
    MvcResult result =
        mvc.perform(get(url))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andReturn();
    MockHttpServletResponse response = result.getResponse();
    assertThat(
        "Lookup file succeeds", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));

    FileModel lookupModel = TestUtils.mapFromJson(response.getContentAsString(), FileModel.class);
    assertTrue("Ingest file equals lookup file", lookupModel.equals(fileModel));

    // Error: Duplicate target file
    ErrorModel errorModel =
        connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
    assertThat("duplicate file error", errorModel.getMessage(), containsString("already exists"));

    // Lookup the file by path
    url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/filesystem/objects";
    result =
        mvc.perform(get(url).param("path", fileModel.getPath()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andReturn();
    response = result.getResponse();
    assertThat(
        "Lookup file by path succeeds",
        HttpStatus.valueOf(response.getStatus()),
        equalTo(HttpStatus.OK));
    lookupModel = TestUtils.mapFromJson(response.getContentAsString(), FileModel.class);
    assertTrue("Ingest file equals lookup file", lookupModel.equals(fileModel));

    // Delete the file and we should be able to create it successfully again
    connectedOperations.deleteTestFile(datasetSummary.getId(), fileModel.getFileId());
    fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
    assertThat("file path matches", fileModel.getPath(), equalTo(fileLoadModel.getTargetPath()));

    // Error: Non-existent source file
    String badfile = "/I am not a file";
    String uribadfile = "gs://" + testConfig.getIngestbucket() + "/" + badfile;
    String badPath = "/dd/files/" + Names.randomizeName("dir") + badfile;

    fileLoadModel =
        new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath(uribadfile)
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(badPath);

    errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
    assertThat(
        "source file does not exist", errorModel.getMessage(), containsString("file not found"));
  }

  // ------ Retry shared lock/unlock tests ---------------

  @Test
  public void retryAndAcquireSharedLock() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    connectedOperations.retryAcquireLockIngestFileSuccess(
        ConnectedOperations.RetryType.lock,
        true,
        true,
        ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT,
        datasetSummary.getId(),
        fileLoadModel,
        configService,
        datasetDao);
  }

  @Test
  public void retryAndAcquireSharedUnlock() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    connectedOperations.retryAcquireLockIngestFileSuccess(
        ConnectedOperations.RetryType.unlock,
        true,
        true,
        ConfigEnum.FILE_INGEST_UNLOCK_RETRY_FAULT,
        datasetSummary.getId(),
        fileLoadModel,
        configService,
        datasetDao);
  }

  // These tests can be used as one offs to see if fatal errors are working as expected
  // But, as is, they shouldn't be run every time because it can leave tests in a bad state
  // (leftover artifacts after tests)
  @Ignore
  @Test
  public void retryAndEventuallyFailToAcquireSharedLock() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    connectedOperations.retryAcquireLockIngestFileSuccess(
        ConnectedOperations.RetryType.lock,
        false,
        false,
        ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT,
        datasetSummary.getId(),
        fileLoadModel,
        configService,
        datasetDao);
    configService.setFault(ConfigEnum.FILE_INGEST_LOCK_RETRY_FAULT.toString(), false);
  }

  @Ignore
  @Test
  public void retryAndFailAcquireSharedUnlock() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    connectedOperations.retryAcquireLockIngestFileSuccess(
        ConnectedOperations.RetryType.unlock,
        false,
        true,
        ConfigEnum.FILE_INGEST_UNLOCK_FATAL_FAULT,
        datasetSummary.getId(),
        fileLoadModel,
        configService,
        datasetDao);
  }

  @Ignore
  @Test
  public void retryAndFailAcquireSharedLock() throws Exception {
    FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

    connectedOperations.retryAcquireLockIngestFileSuccess(
        ConnectedOperations.RetryType.lock,
        false,
        true,
        ConfigEnum.FILE_INGEST_LOCK_FATAL_FAULT,
        datasetSummary.getId(),
        fileLoadModel,
        configService,
        datasetDao);
  }

  static void checkLoadSummary(
      BulkLoadResultModel summary,
      String loadTag,
      int total,
      int succeeded,
      int failed,
      int notTried) {
    assertThat("correct load tag", summary.getLoadTag(), equalTo(loadTag));
    assertThat("correct total files", summary.getTotalFiles(), equalTo(total));
    assertThat("correct succeeded files", summary.getSucceededFiles(), equalTo(succeeded));
    assertThat("correct failed files", summary.getFailedFiles(), equalTo(failed));
    assertThat("correct notTried files", summary.getNotTriedFiles(), equalTo(notTried));
  }

  // We have a static array of good paths and bad paths with their associated
  // target. That lets us build arrays with various numbers of failures and
  // adjust arrays to "fix" broken loads.
  private static String[] goodFileSource =
      new String[] {
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.bam",
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.bam",
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.bam.bai",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.bam",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.bam.bai",
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.bam",
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.bam.bai"
      };
  private static String[] badFileSource =
      new String[] {
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.x",
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.i",
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.x",
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.i",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.x",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.i",
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.x",
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.i"
      };
  private static String[] fileTarget =
      new String[] {
        "/encodefiles/20160707/ENCFF580QIE.bam",
        "/encodefiles/20160707/ENCFF580QIE.bam.bai",
        "/encodefiles/20170824/ENCFF180PCI.bam",
        "/encodefiles/20170824/ENCFF180PCI.bam.bai",
        "/encodefiles/20170824/ENCFF774RTX.bam",
        "/encodefiles/20170824/ENCFF774RTX.bam.bai",
        "/encodefiles/20170824/ENCFF678JJZ.bam",
        "/encodefiles/20170824/ENCFF678JJZ.bam.bai",
        "/encodefiles/20170824/ENCFF591XCX.bam",
        "/encodefiles/20170824/ENCFF591XCX.bam.bai",
        "/encodefiles/20170824/ENCFF912JKA.bam",
        "/encodefiles/20170824/ENCFF912JKA.bam.bai",
        "/encodefiles/20170824/ENCFF097NAZ.bam",
        "/encodefiles/20170824/ENCFF097NAZ.bam.bai",
        "/encodefiles/20180118/ENCFF538GKX.bam",
        "/encodefiles/20180118/ENCFF538GKX.bam.bai",
        "/encodefiles/20180504/ENCFF823AJQ.bam",
        "/encodefiles/20180504/ENCFF823AJQ.bam.bai"
      };

  static BulkLoadFileModel getFileModel(boolean getGood, int index, String testId) {
    assertTrue("test bug: file index not in range", index < fileTarget.length);

    BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");

    String infile = (getGood ? goodFileSource[index] : badFileSource[index]);
    model
        .description("bulk load file " + index)
        .sourcePath(infile)
        .targetPath("/" + testId + fileTarget[index]);
    return model;
  }

  private FileLoadModel makeFileLoad(UUID profileId) {
    String targetDir = Names.randomizeName("dir");
    String uri = "gs://" + testConfig.getIngestbucket() + "/files/" + testPdfFile;
    String targetPath = "/dd/files/" + targetDir + "/" + testPdfFile;

    return new FileLoadModel()
        .sourcePath(uri)
        .description(testDescription)
        .mimeType(testMimeType)
        .targetPath(targetPath)
        .profileId(profileId);
  }
}
