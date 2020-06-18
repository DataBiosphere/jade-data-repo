package bio.terra.service.filedata.google.firestore;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadRequestModel;
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
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.DataLocationSelector;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.util.*;

import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class FileOperationTest {
    @Autowired
    private MockMvc mvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private JsonLoader jsonLoader;
    @Autowired
    private ConnectedTestConfiguration testConfig;
    @Autowired
    private DrsIdService drsService;
    @Autowired
    private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired
    private ConnectedOperations connectedOperations;
    @Autowired
    private ConfigurationService configService;
    @Autowired
    private DatasetDao datasetDao;
    @Autowired
    private DataLocationService dataLocationService;
    @Autowired
    private BigQueryPdao bigQueryPdao;

    @MockBean
    private IamProviderInterface samService;

    @SpyBean
    private DataLocationSelector dataLocationSelector;

    private static Logger logger = LoggerFactory.getLogger(FileOperationTest.class);
    private int validFileCounter;
    private String coreBillingAccountId;
    private BillingProfileModel profileModel;
    private DatasetSummaryModel datasetSummary;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        connectedOperations.stubOutSamCalls(samService);

        // File generator indices
        validFileCounter = 0;

        // Retrieve billing info
        coreBillingAccountId = googleResourceConfiguration.getCoreBillingAccount();
        profileModel = connectedOperations.createProfileForAccount(coreBillingAccountId);

        datasetSummary = connectedOperations.createDataset(profileModel, "snapshot-test-dataset.json");

        // Make sure we start from a known configuration
        configService.reset();
        // Set the configuration so it is constant for the load tests
        ConfigModel concurrentConfig = configService.getConfig(ConfigEnum.LOAD_CONCURRENT_FILES.name());
        concurrentConfig.setParameter(new ConfigParameterModel().value("6"));
        ConfigModel driverWaitConfig = configService.getConfig(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS.name());
        driverWaitConfig.setParameter(new ConfigParameterModel().value("30"));
        ConfigGroupModel configGroupModel = new ConfigGroupModel()
            .label("FileOperationTest")
            .addGroupItem(concurrentConfig)
            .addGroupItem(driverWaitConfig);
        configService.setConfig(configGroupModel);
    }

    @After
    public void teardown() throws Exception {
        configService.reset();
        connectedOperations.teardown();
    }

    private static String testDescription = "test file description";
    private static String testMimeType = "application/pdf";
    private static String testPdfFile = "File Design Notes.pdf";

    @Test
    public void fileOperationsTest() throws Exception {
        FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

        FileModel fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
        assertThat("file path matches", fileModel.getPath(), equalTo(fileLoadModel.getTargetPath()));

        // Change the data location selector, verify that we can still delete the file
        String newBucketName = "bucket-" + UUID.randomUUID().toString();
        doReturn(newBucketName).when(dataLocationSelector).bucketForFile(any());
        connectedOperations.deleteTestFile(datasetSummary.getId(), fileModel.getFileId());
        fileModel = connectedOperations.ingestFileSuccess(datasetSummary.getId(), fileLoadModel);
        assertThat("file path reflects new bucket location",
            fileModel.getFileDetail().getAccessUrl(),
            containsString(newBucketName));
        // Track the bucket so connected ops can remove it on teardown
        connectedOperations.addBucket(newBucketName);

        // lookup the file we just created
        String url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/files/" + fileModel.getFileId();
        MvcResult result = mvc.perform(get(url))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andReturn();
        MockHttpServletResponse response = result.getResponse();
        assertThat("Lookup file succeeds", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));

        FileModel lookupModel = TestUtils.mapFromJson(response.getContentAsString(), FileModel.class);
        assertTrue("Ingest file equals lookup file", lookupModel.equals(fileModel));

        // Error: Duplicate target file
        ErrorModel errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("duplicate file error", errorModel.getMessage(),
            containsString("already exists"));

        // Lookup the file by path
        url = "/api/repository/v1/datasets/" + datasetSummary.getId() + "/filesystem/objects";
        result = mvc.perform(get(url)
            .param("path", fileModel.getPath()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andReturn();
        response = result.getResponse();
        assertThat("Lookup file by path succeeds", HttpStatus.valueOf(response.getStatus()), equalTo(HttpStatus.OK));
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

        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath(uribadfile)
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(badPath);

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("source file does not exist", errorModel.getMessage(),
            containsString("file not found"));

        // Error: Invalid gs path - case 1: not gs
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("http://jade_notabucket/foo/bar.txt")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("Not a gs schema", errorModel.getMessage(),
            containsString("not a gs"));

        // Error: Invalid gs path - case 2: invalid bucket name
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("gs://jade_notabucket:1234/foo/bar.txt")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("Invalid bucket name", errorModel.getMessage(),
            containsString("Invalid bucket name"));

        // Error: Invalid gs path - case 3: no bucket or path
        fileLoadModel = new FileLoadModel()
            .profileId(profileModel.getId())
            .sourcePath("gs:///")
            .description(testDescription)
            .mimeType(testMimeType)
            .targetPath(makeValidUniqueFilePath());

        errorModel = connectedOperations.ingestFileFailure(datasetSummary.getId(), fileLoadModel);
        assertThat("No bucket or path", errorModel.getMessage(),
            containsString("gs path"));
    }

    @Test
    public void retryAndAcquireSharedLock() throws Exception {
        FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

        connectedOperations.retryAcquireLockIngestFileSuccess(
            true,
            datasetSummary.getId(), fileLoadModel, configService, datasetDao);
    }

    @Test
    public void retryAndFailAcquireSharedLock() throws Exception {
        FileLoadModel fileLoadModel = makeFileLoad(profileModel.getId());

        connectedOperations.retryAcquireLockIngestFileSuccess(
            false,
            datasetSummary.getId(), fileLoadModel, configService, datasetDao);
    }

    // -- array bulk load --

    @Test
    public void arrayMultiFileLoadSuccessTest() throws Exception {
        int fileCount = 10;

        // Test copying load_history data in chunks
        ConfigModel loadHistoryChunkSize = configService.getConfig(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE.name());
        loadHistoryChunkSize.setParameter(new ConfigParameterModel().value("2"));
        ConfigGroupModel configGroupModel = new ConfigGroupModel()
            .label("FileOperationTestMultiFileLoad")
            .addGroupItem(loadHistoryChunkSize);
        configService.setConfig(configGroupModel);

        BulkLoadArrayRequestModel arrayLoad = makeSuccessArrayLoad("arrayMultiFileLoadSuccessTest", 0, fileCount);

        BulkLoadArrayResultModel result = connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
        checkLoadSummary(result.getLoadSummary(), arrayLoad.getLoadTag(), fileCount, fileCount, 0, 0);

        Map<String, String> fileIdMap = new HashMap<>();
        for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
            checkFileResultSuccess(fileResult);
            fileIdMap.put(fileResult.getTargetPath(), fileResult.getFileId());
        }

        // Query Big Query datarepo_load_history table - should reflect all files loaded above
        String columnToQuery = "file_id";
        TableResult queryLoadHistoryTableResult = queryLoadHistoryTable(columnToQuery);
        ArrayList<String> ids = new ArrayList<>();
        queryLoadHistoryTableResult.iterateAll().forEach(r -> ids.add(r.get(columnToQuery).getStringValue()));

        assertThat("Number of files in datarepo_load_history table match load summary", ids.size(), equalTo(fileCount));
        for (String bq_file_id:ids) {
            assertNotNull("fileIdMap should contain File_id from datarepo_load_history",
                fileIdMap.containsValue(bq_file_id));
        }

        // retry successful load to make sure it still succeeds and does nothing
        BulkLoadArrayResultModel result2 = connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
        checkLoadSummary(result2.getLoadSummary(), arrayLoad.getLoadTag(), fileCount, fileCount, 0, 0);

        for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
            checkFileResultSuccess(fileResult);
            assertThat("FileId matches", fileResult.getFileId(), equalTo(fileIdMap.get(fileResult.getTargetPath())));
        }
    }

    // Get the count of rows in a table or view
    private TableResult queryLoadHistoryTable(String columns) throws Exception {
        return TestUtils.selectFromBigQueryDataset(
            bigQueryPdao, datasetDao, dataLocationService, datasetSummary.getName(), PDAO_LOAD_HISTORY_TABLE, columns);
    }

    @Test
    public void arrayMultiFileLoadDoubleSuccessTest() throws Exception {
        int fileCount = 8;
        int totalfileCount = fileCount * 2;
        BulkLoadArrayRequestModel arrayLoad1 = makeSuccessArrayLoad("arrayMultiDoubleSuccess", 0, fileCount);
        BulkLoadArrayRequestModel arrayLoad2 = makeSuccessArrayLoad("arrayMultiDoubleSuccess", fileCount, fileCount);
        String loadTag1 = arrayLoad1.getLoadTag();
        String loadTag2 = arrayLoad2.getLoadTag();
        String datasetId = datasetSummary.getId();

        MvcResult result1 = connectedOperations.ingestArrayRaw(datasetId, arrayLoad1);
        MvcResult result2 = connectedOperations.ingestArrayRaw(datasetId, arrayLoad2);

        MockHttpServletResponse response1 = connectedOperations.validateJobModelAndWait(result1);
        MockHttpServletResponse response2 = connectedOperations.validateJobModelAndWait(result2);

        BulkLoadArrayResultModel resultModel1 =
            connectedOperations.handleSuccessCase(response1, BulkLoadArrayResultModel.class);

        BulkLoadArrayResultModel resultModel2 =
            connectedOperations.handleSuccessCase(response2, BulkLoadArrayResultModel.class);

        checkLoadSummary(resultModel1.getLoadSummary(), loadTag1, fileCount, fileCount, 0, 0);
        checkLoadSummary(resultModel2.getLoadSummary(), loadTag2, fileCount, fileCount, 0, 0);

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
        queryLoadHistoryTableResult.iterateAll().forEach(r -> bq_fileIds.add(r.get(columnToQuery).getStringValue()));

        assertThat("Number of files in datarepo_load_history table match load summary",
            totalfileCount, equalTo(bq_fileIds.size()));
        for (String bq_file_id:bq_fileIds) {
            assertNotNull("fileIdMap should contain File_id from datarepo_load_history",
                fileIds.contains(bq_file_id));
        }
    }

    @Test
    public void arrayMultiFileLoadFailRetryTest() throws Exception {
        String testId = Names.randomizeName("test");
        String loadTag = "arrayMultiFileLoadFileRetryTest" + testId;
        BulkLoadArrayRequestModel arrayLoad = new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(2);
        arrayLoad.addLoadArrayItem(getFileModel(true, 2, testId));
        arrayLoad.addLoadArrayItem(getFileModel(false, 3, testId));
        arrayLoad.addLoadArrayItem(getFileModel(true, 4, testId));

        BulkLoadArrayResultModel result = connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
        checkLoadSummary(result.getLoadSummary(), loadTag, 3, 2, 1, 0);

        Map<String, BulkLoadFileResultModel> resultMap = new HashMap<>();
        for (BulkLoadFileResultModel fileResult : result.getLoadFileResults()) {
            resultMap.put(fileResult.getTargetPath(), fileResult);
        }
        // Query Big Query datarepo_load_history table - assert correctly reflects different
        // bulk load file states
        String columnsToQuery = "state, file_id, error";
        TableResult queryLoadHistoryTableResult = queryLoadHistoryTable(columnsToQuery);
        for (FieldValueList item:queryLoadHistoryTableResult.getValues()) {
            String state = item.get(0).getStringValue();
            assertTrue("state should either be succeeded or failed.",
                state.equals(BulkLoadFileState.SUCCEEDED.toString()) ||
                    state.equals(BulkLoadFileState.FAILED.toString()));
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
        loadArray.set(1, getFileModel(true, 3, testId));
        BulkLoadArrayResultModel result2 = connectedOperations.ingestArraySuccess(datasetSummary.getId(), arrayLoad);
        checkLoadSummary(result2.getLoadSummary(), loadTag, 3, 3, 0, 0);
    }

    @Test
    public void arrayMultiFileLoadExceedMaxTest() throws Exception {
        // Set the allowed array files to a very small value so we can easily hit the error
        ConfigModel bulkArrayMaxConfig = configService.getConfig(ConfigEnum.LOAD_BULK_ARRAY_FILES_MAX.name());
        bulkArrayMaxConfig.setParameter(new ConfigParameterModel().value("5"));
        ConfigGroupModel configGroupModel = new ConfigGroupModel()
            .label("FileOperationTest:loadExceedMax")
            .addGroupItem(bulkArrayMaxConfig);
        configService.setConfig(configGroupModel);

        String testId = Names.randomizeName("test");
        String loadTag = "arrayMultiFileLoadExceedMaxTest" + testId;
        BulkLoadArrayRequestModel arrayLoad = new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
        for (int i = 0; i < 8; i++) {
            arrayLoad.addLoadArrayItem(getFileModel(true, i, testId));
        }

        MvcResult result = connectedOperations.ingestArrayRaw(datasetSummary.getId(), arrayLoad);
        assertThat("Got bad request", result.getResponse().getStatus(), equalTo(HttpStatus.BAD_REQUEST.value()));
    }

    // -- file bulk load --
    @Test
    public void multiFileLoadSuccessTest() throws Exception {
        BulkLoadRequestModel loadRequest =
            makeBulkFileLoad("multiFileLoadSuccessTest", 0, 0, false, new boolean[]{true, true, true, true});

        BulkLoadResultModel result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
        checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);

        // retry successful load to make sure it still succeeds and does nothing
        result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
        checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);
    }

    @Test
    public void multiFileLoadFailRetryTest() throws Exception {
        BulkLoadRequestModel loadRequest =
            makeBulkFileLoad("multiFileLoadFailRetry", 0, 0, false, new boolean[]{true, false, true, false});
        loadRequest.maxFailedFileLoads(4);
        String loadTag = loadRequest.getLoadTag();

        BulkLoadResultModel result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
        checkLoadSummary(result, loadTag, 4, 2, 2, 0);

        loadRequest =
            makeBulkFileLoad("multiFileLoadFailRetry", 0, 0, false, new boolean[]{true, true, true, true});
        loadRequest.loadTag(loadTag);
        result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
        checkLoadSummary(result, loadTag, 4, 4, 0, 0);
    }

    @Test
    public void multiFileLoadSuccessExtraKeysTest() throws Exception {
        BulkLoadRequestModel loadRequest =
            makeBulkFileLoad("multiFileLoadSuccessExtraKeys", 0, 0, true, new boolean[]{true, true, true, true});

        BulkLoadResultModel result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
        checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);
    }

    @Test
    public void multiFileLoadBadLineTest() throws Exception {
        // part 1: test that we exit with the bad line error when we have fewer than the max
        BulkLoadRequestModel loadRequest =
            makeBulkFileLoad("multiFileLoadBadLineSuccess", 0, 3, false, new boolean[]{true, false, true, false});
        loadRequest.maxFailedFileLoads(4);

        ErrorModel errorModel = connectedOperations.ingestBulkFileFailure(datasetSummary.getId(), loadRequest);
        assertThat("Expected error", errorModel.getMessage(), containsString("bad lines in the control file"));
        assertThat("Expected error", errorModel.getMessage(), containsString("There were"));
        assertThat("Expected number of error details", errorModel.getErrorDetail().size(), equalTo(3));

        // part 2: test that we exit with bad line error when we have more than the max
        loadRequest =
            makeBulkFileLoad("multiFileLoadBadLineSuccess", 0, 6, false, new boolean[]{true, true, true, true});
        loadRequest.maxFailedFileLoads(4);

        errorModel = connectedOperations.ingestBulkFileFailure(datasetSummary.getId(), loadRequest);
        assertThat("Expected error", errorModel.getMessage(), containsString("bad lines in the control file"));
        assertThat("Expected error", errorModel.getMessage(), containsString("More than"));
        assertThat("Expected number of error details", errorModel.getErrorDetail().size(), greaterThan(5));
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

    private static void checkLoadSummary(BulkLoadResultModel summary,
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

    private BulkLoadArrayRequestModel makeSuccessArrayLoad(String tagBase, int startIndex, int fileCount) {
        String testId = Names.randomizeName("test");
        String loadTag = tagBase + testId;
        BulkLoadArrayRequestModel arrayLoad = new BulkLoadArrayRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
        for (int index = startIndex; index < startIndex + fileCount; index++) {
            arrayLoad.addLoadArrayItem(getFileModel(true, index, testId));
        }
        return arrayLoad;
    }

    private BulkLoadRequestModel makeBulkFileLoad(String tagBase,
                                                  int startIndex,
                                                  int badLines,
                                                  boolean addExtraKeys,
                                                  boolean[] validPattern) {
        int fileCount = validPattern.length;
        String testId = Names.randomizeName("test");
        String loadTag = tagBase + testId;
        String targetPath = "scratch/controlfile" + UUID.randomUUID().toString() + ".json";
        connectedOperations.addScratchFile(targetPath); // track the file so it gets cleaned up

        String gspath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;
        Storage storage = StorageOptions.getDefaultInstance().getService();


        try (GcsChannelWriter writer = new GcsChannelWriter(storage, testConfig.getIngestbucket(), targetPath)) {
            for (int i = 0; i < badLines; i++) {
                String badLine = "bad line: " + loadTag + "\n";
                writer.write(badLine);
            }

            for (int i = 0; i < fileCount; i++) {
                BulkLoadFileModel fileModel = getFileModel(validPattern[i], startIndex + i, testId);
                String fileLine = objectMapper.writeValueAsString(fileModel) + "\n";
                // Inject extra key-value pairs into file lines
                if (addExtraKeys) {
                    fileLine = fileLine.replaceFirst("^\\{", "{\"customKey\":\"customValue\",");
                    logger.info("Added extra keys: " + fileLine);
                }
                writer.write(fileLine);
            }
        } catch (IOException ex) {
            fail("Failed to write load file '" + targetPath +
                "' to bucket '" + testConfig.getIngestbucket() + "'");
        }

        BulkLoadRequestModel loadRequest = new BulkLoadRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0)
            .loadControlFile(gspath);
        return loadRequest;
    }

    // We have a static array of good paths and bad paths with their associated
    // target. That lets us build arrays with various numbers of failures and
    // adjust arrays to "fix" broken loads.
    private static String[] goodFileSource = new String[]{
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
    private static String[] badFileSource = new String[]{
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
    private static String[] fileTarget = new String[]{
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

    private BulkLoadFileModel getFileModel(boolean getGood, int index, String testId) {
        assertTrue("test bug: file index not in range", index < fileTarget.length);

        BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");

        String infile = (getGood ? goodFileSource[index] : badFileSource[index]);
        model.description("bulk load file " + index)
            .sourcePath(infile)
            .targetPath(testId + fileTarget[index]);
        return model;
    }

    private String makeValidUniqueFilePath() {
        validFileCounter++;
        return String.format("/dd/files/foo/ValidFileName%d.pdf", validFileCounter);
    }

    private FileLoadModel makeFileLoad(String profileId) {
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
