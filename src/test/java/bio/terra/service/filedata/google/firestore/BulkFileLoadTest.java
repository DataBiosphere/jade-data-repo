package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class BulkFileLoadTest {

  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ObjectMapper objectMapper;

  @MockBean private IamProviderInterface samService;

  private static final Logger logger = LoggerFactory.getLogger(BulkFileLoadTest.class);
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
  public void multiFileLoadSuccessTest() throws Exception {
    BulkLoadRequestModel loadRequest =
        makeBulkFileLoad(
            "multiFileLoadSuccessTest", 0, 0, false, new boolean[] {true, true, true, true});

    BulkLoadResultModel result =
        connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    FileOperationTest.checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);

    // retry successful load to make sure it still succeeds and does nothing
    result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    FileOperationTest.checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);
  }

  @Test
  public void multiFileLoadFailRetryTest() throws Exception {
    BulkLoadRequestModel loadRequest =
        makeBulkFileLoad(
            "multiFileLoadFailRetry", 0, 0, false, new boolean[] {true, false, true, false});
    loadRequest.maxFailedFileLoads(4);
    String loadTag = loadRequest.getLoadTag();

    BulkLoadResultModel result =
        connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    FileOperationTest.checkLoadSummary(result, loadTag, 4, 2, 2, 0);

    loadRequest =
        makeBulkFileLoad(
            "multiFileLoadFailRetry", 0, 0, false, new boolean[] {true, true, true, true});
    loadRequest.loadTag(loadTag);
    result = connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    FileOperationTest.checkLoadSummary(result, loadTag, 4, 4, 0, 0);
  }

  @Test
  public void multiFileLoadSuccessExtraKeysTest() throws Exception {
    BulkLoadRequestModel loadRequest =
        makeBulkFileLoad(
            "multiFileLoadSuccessExtraKeys", 0, 0, true, new boolean[] {true, true, true, true});

    BulkLoadResultModel result =
        connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    FileOperationTest.checkLoadSummary(result, loadRequest.getLoadTag(), 4, 4, 0, 0);
  }

  @Test
  public void multiFileLoadBadLineTest() throws Exception {
    // part 1: test that we exit with the bad line error when we have fewer than the max
    BulkLoadRequestModel loadRequest =
        makeBulkFileLoad(
            "multiFileLoadBadLineSuccess", 0, 3, false, new boolean[] {true, false, true, false});
    loadRequest.maxFailedFileLoads(0);

    ErrorModel errorModel =
        connectedOperations.ingestBulkFileFailure(datasetSummary.getId(), loadRequest);
    assertThat(
        "Expected error",
        errorModel.getMessage(),
        containsString("Invalid lines in the control file"));
    assertThat("Expected number of error details", errorModel.getErrorDetail().size(), equalTo(3));

    // part 2: test that we exit with bad line error when we have more than the max
    loadRequest =
        makeBulkFileLoad(
            "multiFileLoadBadLineSuccess", 0, 9, false, new boolean[] {true, true, true, true});
    loadRequest.maxFailedFileLoads(0);

    errorModel = connectedOperations.ingestBulkFileFailure(datasetSummary.getId(), loadRequest);
    assertThat(
        "Expected error",
        errorModel.getMessage(),
        containsString("Invalid lines in the control file"));
    // We should see the number of errors returned equal to maxBadLoadFileLineErrorsReported (set to
    // 5 in app config)
    // + 1 to indicate that the error details were truncated
    assertThat("Expected number of error details", errorModel.getErrorDetail().size(), equalTo(6));
    assertThat(
        "Expected error",
        errorModel.getErrorDetail().get(5),
        containsString("Error details truncated"));
  }

  private BulkLoadRequestModel makeBulkFileLoad(
      String tagBase, int startIndex, int badLines, boolean addExtraKeys, boolean[] validPattern) {
    int fileCount = validPattern.length;
    String testId = Names.randomizeName("test");
    String loadTag = tagBase + testId;
    String targetPath = "scratch/controlfile" + UUID.randomUUID() + ".json";
    connectedOperations.addScratchFile(targetPath); // track the file so it gets cleaned up

    String gspath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;
    Storage storage = StorageOptions.getDefaultInstance().getService();

    try (GcsChannelWriter writer =
        new GcsChannelWriter(storage, testConfig.getIngestbucket(), targetPath)) {
      for (int i = 0; i < badLines; i++) {
        String badLine = "bad line: " + loadTag;
        writer.writeLine(badLine);
      }

      for (int i = 0; i < fileCount; i++) {
        BulkLoadFileModel fileModel =
            FileOperationTest.getFileModel(validPattern[i], startIndex + i, testId);
        String fileLine = objectMapper.writeValueAsString(fileModel);
        // Inject extra key-value pairs into file lines
        if (addExtraKeys) {
          fileLine = fileLine.replaceFirst("^\\{", "{\"customKey\":\"customValue\",");
          logger.info("Added extra keys: " + fileLine);
        }
        writer.writeLine(fileLine);
      }
    } catch (IOException ex) {
      fail(
          "Failed to write load file '"
              + targetPath
              + "' to bucket '"
              + testConfig.getIngestbucket()
              + "'");
    }

    BulkLoadRequestModel loadRequest =
        new BulkLoadRequestModel()
            .profileId(profileModel.getId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0)
            .loadControlFile(gspath);
    return loadRequest;
  }
}
