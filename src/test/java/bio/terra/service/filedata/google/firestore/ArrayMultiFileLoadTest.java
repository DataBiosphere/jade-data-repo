package bio.terra.service.filedata.google.firestore;

import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.ResourceService;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class ArrayMultiFileLoadTest {

  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private DatasetDao datasetDao;
  @Autowired private ResourceService dataLocationService;
  @Autowired private ConnectedTestConfiguration testConfig;

  @MockBean private IamProviderInterface samService;

  private static final Logger logger = LoggerFactory.getLogger(ArrayMultiFileLoadTest.class);
  private BillingProfileModel profileModel;
  private DatasetSummaryModel datasetSummary;

  @Before
  public void setup() throws Exception {
    // Setup mock sam service
    connectedOperations.stubOutSamCalls(samService);

    // Retrieve billing info
    String coreBillingAccountId = testConfig.getGoogleBillingAccountId();
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

  @Test
  public void arrayMultiFileLoadSuccessTest() throws Exception {
    int fileCount = 10;

    // Test copying load_history data in chunks
    ConfigModel loadHistoryChunkSize =
        configService.getConfig(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE.name());
    loadHistoryChunkSize.setParameter(new ConfigParameterModel().value("2"));
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel()
            .label("FileOperationTestMultiFileLoad")
            .addGroupItem(loadHistoryChunkSize);
    configService.setConfig(configGroupModel);

    BulkLoadArrayRequestModel arrayLoad =
        makeSuccessArrayLoad("arrayMultiFileLoadSuccessTest", 0, fileCount);

    BulkLoadArrayResultModel result =
        connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
    FileOperationTest.checkLoadSummary(
        result.getLoadSummary(), arrayLoad.getLoadTag(), fileCount, fileCount, 0, 0);

    var loadHistoryList =
        connectedOperations.getLoadHistory(
            datasetSummary.getId(), result.getLoadSummary().getLoadTag(), 0, 20);

    assertThat(
        "The correct number of load history entries are returned",
        loadHistoryList.getTotal(),
        equalTo(fileCount));

    assertThat(
        "getting load history has the same items as response from bulk file load",
        loadHistoryList.getItems().stream()
            .map(TestUtils::toBulkLoadFileResultModel)
            .collect(Collectors.toSet()),
        Matchers.equalTo(Set.copyOf(result.getLoadFileResults())));

    Map<String, String> fileIdMap = new HashMap<>();
    for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
      checkFileResultSuccess(fileResult);
      fileIdMap.put(fileResult.getTargetPath(), fileResult.getFileId());
    }

    // Query Big Query datarepo_load_history table - should reflect all files loaded above
    String columnToQuery = "file_id";
    TableResult queryLoadHistoryTableResult = queryLoadHistoryTable(columnToQuery);
    ArrayList<String> ids = new ArrayList<>();
    queryLoadHistoryTableResult
        .iterateAll()
        .forEach(r -> ids.add(r.get(columnToQuery).getStringValue()));

    assertThat(
        "Number of files in datarepo_load_history table match load summary",
        ids.size(),
        equalTo(fileCount));
    for (String bq_file_id : ids) {
      assertNotNull(
          "fileIdMap should contain File_id from datarepo_load_history",
          fileIdMap.containsValue(bq_file_id));
    }

    // retry successful load to make sure it still succeeds and does nothing
    BulkLoadArrayResultModel result2 =
        connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
    FileOperationTest.checkLoadSummary(
        result2.getLoadSummary(), arrayLoad.getLoadTag(), fileCount, fileCount, 0, 0);

    for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
      checkFileResultSuccess(fileResult);
      assertThat(
          "FileId matches",
          fileResult.getFileId(),
          equalTo(fileIdMap.get(fileResult.getTargetPath())));
    }
  }

  // Get the count of rows in a table or view
  private TableResult queryLoadHistoryTable(String columns) throws Exception {
    return TestUtils.selectFromBigQueryDataset(
        datasetDao,
        dataLocationService,
        datasetSummary.getName(),
        PDAO_LOAD_HISTORY_TABLE,
        columns);
  }

  @Test
  public void arrayMultiFileLoadDoubleSuccessTest() throws Exception {
    int fileCount = 8;
    int totalfileCount = fileCount * 2;
    BulkLoadArrayRequestModel arrayLoad1 =
        makeSuccessArrayLoad("arrayMultiDoubleSuccess", 0, fileCount);
    BulkLoadArrayRequestModel arrayLoad2 =
        makeSuccessArrayLoad("arrayMultiDoubleSuccess", fileCount, fileCount);
    String loadTag1 = arrayLoad1.getLoadTag();
    String loadTag2 = arrayLoad2.getLoadTag();
    UUID datasetId = datasetSummary.getId();

    MvcResult result1 = connectedOperations.ingestArrayRaw(datasetId, arrayLoad1);
    MvcResult result2 = connectedOperations.ingestArrayRaw(datasetId, arrayLoad2);

    MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
    MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);

    BulkLoadArrayResultModel resultModel1 =
        connectedOperations.handleSuccessCase(response1, BulkLoadArrayResultModel.class);

    BulkLoadArrayResultModel resultModel2 =
        connectedOperations.handleSuccessCase(response2, BulkLoadArrayResultModel.class);

    FileOperationTest.checkLoadSummary(
        resultModel1.getLoadSummary(), loadTag1, fileCount, fileCount, 0, 0);
    FileOperationTest.checkLoadSummary(
        resultModel2.getLoadSummary(), loadTag2, fileCount, fileCount, 0, 0);

    List<String> fileIds = new ArrayList<>();
    for (BulkLoadFileResultModel fileResult : resultModel1.getLoadFileResults()) {
      checkFileResultSuccess(fileResult);
      fileIds.add(fileResult.getFileId());
    }
    for (BulkLoadFileResultModel fileResult : resultModel2.getLoadFileResults()) {
      checkFileResultSuccess(fileResult);
      fileIds.add(fileResult.getFileId());
    }

    // Query Big Query datarepo_load_history table - should reflect all files loaded above
    String columnToQuery = "file_id";
    TableResult queryLoadHistoryTableResult = queryLoadHistoryTable(columnToQuery);
    ArrayList<String> bq_fileIds = new ArrayList<>();
    queryLoadHistoryTableResult
        .iterateAll()
        .forEach(r -> bq_fileIds.add(r.get(columnToQuery).getStringValue()));

    assertThat(
        "Number of files in datarepo_load_history table match load summary",
        totalfileCount,
        equalTo(bq_fileIds.size()));
    for (String bq_file_id : bq_fileIds) {
      assertNotNull(
          "fileIdMap should contain File_id from datarepo_load_history",
          fileIds.contains(bq_file_id));
    }
  }

  @Test
  public void arrayMultiFileLoadFailRetryTest() throws Exception {
    String testId = Names.randomizeName("test");
    String loadTag = "arrayMultiFileLoadFileRetryTest" + testId;
    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(2);
    arrayLoad.addLoadArrayItem(FileOperationTest.getFileModel(true, 2, testId));
    arrayLoad.addLoadArrayItem(FileOperationTest.getFileModel(false, 3, testId));
    arrayLoad.addLoadArrayItem(FileOperationTest.getFileModel(true, 4, testId));

    BulkLoadArrayResultModel result =
        connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
    FileOperationTest.checkLoadSummary(result.getLoadSummary(), loadTag, 3, 2, 1, 0);

    Map<String, BulkLoadFileResultModel> resultMap = new HashMap<>();
    for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
      resultMap.put(fileResult.getTargetPath(), fileResult);
    }
    // Query Big Query datarepo_load_history table - assert correctly reflects different
    // bulk load file states
    String columnsToQuery = "state, file_id, error";
    TableResult queryLoadHistoryTableResult = queryLoadHistoryTable(columnsToQuery);
    for (FieldValueList item : queryLoadHistoryTableResult.getValues()) {
      String state = item.get(0).getStringValue();
      assertTrue(
          "state should either be succeeded or failed.",
          state.equals(BulkLoadFileState.SUCCEEDED.toString())
              || state.equals(BulkLoadFileState.FAILED.toString()));
      if (state.equals(BulkLoadFileState.SUCCEEDED.toString())) {
        assertTrue("file_id should have value", item.get(1).getStringValue().length() > 0);
        assertTrue("Error column should be empty", item.get(2).getStringValue().length() == 0);
      } else if (state.equals(BulkLoadFileState.FAILED.toString())) {
        assertTrue("file_id should NOT have value", item.get(1).getStringValue().length() == 0);
        assertTrue("Error column should have value", item.get(2).getStringValue().length() > 0);
      }
    }
    FieldValueList curr_result;

    List<BulkLoadFileModel> loadArray = arrayLoad.getLoadArray();
    BulkLoadFileResultModel fileResult = resultMap.get(loadArray.get(0).getTargetPath());
    checkFileResultSuccess(fileResult);

    fileResult = resultMap.get(loadArray.get(1).getTargetPath());
    checkFileResultFailed(fileResult);

    fileResult = resultMap.get(loadArray.get(2).getTargetPath());
    checkFileResultSuccess(fileResult);

    // fix the bad file and retry load
    loadArray.set(1, FileOperationTest.getFileModel(true, 3, testId));
    BulkLoadArrayResultModel result2 =
        connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
    FileOperationTest.checkLoadSummary(result2.getLoadSummary(), loadTag, 3, 3, 0, 0);
  }

  @Test
  public void arrayMultiFileLoadExceedMaxTest() throws Exception {
    // Set the allowed array files to a very small value so we can easily hit the error
    ConfigModel bulkArrayMaxConfig =
        configService.getConfig(ConfigEnum.LOAD_BULK_ARRAY_FILES_MAX.name());
    bulkArrayMaxConfig.setParameter(new ConfigParameterModel().value("5"));
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel()
            .label("FileOperationTest:loadExceedMax")
            .addGroupItem(bulkArrayMaxConfig);
    configService.setConfig(configGroupModel);

    String testId = Names.randomizeName("test");
    String loadTag = "arrayMultiFileLoadExceedMaxTest" + testId;
    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
    for (int i = 0; i < 8; i++) {
      arrayLoad.addLoadArrayItem(FileOperationTest.getFileModel(true, i, testId));
    }

    MvcResult result = connectedOperations.ingestArrayRaw(datasetSummary.getId(), arrayLoad);
    assertThat(
        "Got bad request",
        result.getResponse().getStatus(),
        equalTo(HttpStatus.BAD_REQUEST.value()));
  }

  private static void checkFileResultFailed(BulkLoadFileResultModel fileResult) {
    assertNotNull("Error is not null", fileResult.getError());
    assertNull("FileId is null", fileResult.getFileId());
    assertThat("State is FAILED", fileResult.getState(), equalTo(BulkLoadFileState.FAILED));
  }

  private static void checkFileResultSuccess(BulkLoadFileResultModel fileResult) {
    assertNull("Error is null", fileResult.getError());
    assertNotNull("FileId is not null", fileResult.getFileId());
    assertThat("State is SUCCEEDED", fileResult.getState(), equalTo(BulkLoadFileState.SUCCEEDED));
  }

  private BulkLoadArrayRequestModel makeSuccessArrayLoad(
      String tagBase, int startIndex, int fileCount) {
    String testId = Names.randomizeName("test");
    String loadTag = tagBase + testId;
    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
    for (int index = startIndex; index < startIndex + fileCount; index++) {
      arrayLoad.addLoadArrayItem(FileOperationTest.getFileModel(true, index, testId));
    }
    return arrayLoad;
  }
}
