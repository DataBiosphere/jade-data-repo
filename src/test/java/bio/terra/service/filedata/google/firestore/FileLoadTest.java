package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.google.gcs.GcsChannelWriter;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
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
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class FileLoadTest {
  private static final Logger logger = LoggerFactory.getLogger(FileLoadTest.class);

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private DrsIdService drsService;
  @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConfigurationService configService;

  @MockBean private IamProviderInterface samService;

  @SpyBean private GoogleProjectService projectService;

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
  }

  @After
  public void teardown() throws Exception {
    connectedOperations.teardown();
  }

  // Driver to push a lot of file loads into the system without actually doing any loads.
  @Test
  public void fileLoadTest() throws Exception {
    // Test configuration
    String concurrentFiles = "20"; // number of files to load in parallel
    String driverWaitSecs = "1"; // time for driver to wait for loads to complete
    int filesToLoad = 40; // number of files to fake load

    configService.reset();
    ConfigModel concurrentConfig = configService.getConfig(ConfigEnum.LOAD_CONCURRENT_FILES.name());
    concurrentConfig.setParameter(new ConfigParameterModel().value(concurrentFiles));
    ConfigModel driverWaitConfig =
        configService.getConfig(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS.name());
    driverWaitConfig.setParameter(new ConfigParameterModel().value(driverWaitSecs));
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel()
            .label("FileLoadTest")
            .addGroupItem(concurrentConfig)
            .addGroupItem(driverWaitConfig);
    configService.setConfig(configGroupModel);
    configService.setFault(ConfigEnum.LOAD_SKIP_FILE_LOAD.name(), true);

    long startTime = System.currentTimeMillis();
    BulkLoadRequestModel loadRequest = makeBulkFileLoad("fileLoadTest", filesToLoad);

    BulkLoadResultModel summary =
        connectedOperations.ingestBulkFileSuccess(datasetSummary.getId(), loadRequest);
    long endTime = System.currentTimeMillis();
    long elapsedSecs = (endTime - startTime);

    logger.info("elapsed milliseconds    = " + elapsedSecs);
    logger.info("correct load tag        = " + summary.getLoadTag());
    logger.info("correct total files     = " + summary.getTotalFiles());
    logger.info("correct succeeded files = " + summary.getSucceededFiles());
    logger.info("correct failed files    = " + summary.getFailedFiles());
    logger.info("correct notTried files  = " + summary.getNotTriedFiles());

    // delete the dataset within this test, instead of in teardown
    // so that the LOAD_SKIP_FILE_LOAD fault is still enabled and we don't try to delete a file
    // that was never actually copied to GCS
    connectedOperations.deleteTestDatasetAndCleanup(datasetSummary.getId());

    assertThat(summary.getSucceededFiles(), equalTo(filesToLoad));
  }

  private BulkLoadRequestModel makeBulkFileLoad(String tagBase, int fileCount) {
    String testId = Names.randomizeName("test");
    String loadTag = tagBase + testId;
    String targetPath = "scratch/loadtest" + UUID.randomUUID() + ".json";
    connectedOperations.addScratchFile(targetPath); // track the file so it gets cleaned up

    String gspath = "gs://" + testConfig.getIngestbucket() + "/" + targetPath;
    Storage storage = StorageOptions.getDefaultInstance().getService();

    try (GcsChannelWriter writer =
        new GcsChannelWriter(storage, testConfig.getIngestbucket(), targetPath)) {
      int repeats = fileCount / goodFileSource.length; // length is 20

      for (int r = 0; r < repeats; r++) {
        for (int i = 0; i < goodFileSource.length; i++) {
          BulkLoadFileModel fileModel = getFileModel(i, r, testId);
          String fileLine = objectMapper.writeValueAsString(fileModel);
          writer.writeLine(fileLine);
        }
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

  private BulkLoadFileModel getFileModel(int index, int repeat, String testId) {
    assertTrue("test bug: file index not in range", index < fileTarget.length);
    BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
    String infile = goodFileSource[index] + repeat;
    model
        .description("bulk load file " + index)
        .sourcePath(infile)
        .targetPath("/" + testId + fileTarget[index] + repeat);
    return model;
  }
  // We have a static array of good paths and bad paths with their associated
  // target. That lets us build arrays with various numbers of failures and
  // adjust arrays to "fix" broken loads.
  private static String[] goodFileSource =
      new String[] {
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.bam", // 17GB
        "gs://jade-testdata/encodetest/files/2016/07/07/1fd31802-0ea3-4b75-961e-2fd9ac27a15c/ENCFF580QIE.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.bam", // 2.7
        "gs://jade-testdata/encodetest/files/2017/08/24/80317b07-7e78-4223-a3a2-84991c3104be/ENCFF180PCI.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.bam", // 1.9
        "gs://jade-testdata/encodetest/files/2017/08/24/807541ec-51e2-4aea-999f-ce600df9cdc7/ENCFF774RTX.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.bam", // 16
        "gs://jade-testdata/encodetest/files/2017/08/24/8f198dd1-c2a4-443a-b4af-7ef2a0707e12/ENCFF678JJZ.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.bam", // 3.3
        "gs://jade-testdata/encodetest/files/2017/08/24/ac0d9343-0435-490b-aa5d-2f14e8275a9e/ENCFF591XCX.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.bam", // 12.5
        "gs://jade-testdata/encodetest/files/2017/08/24/cd3df621-4696-4fae-a2fc-2c666cafa5e2/ENCFF912JKA.bam.bai",
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.bam", // 14
        "gs://jade-testdata/encodetest/files/2017/08/24/d8fc70e5-2a02-49b3-bdcd-4eccf1fb4406/ENCFF097NAZ.bam.bai",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF168GKX.bam", // 11
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF168GKX.bam.bai",
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.bam", // 11
        "gs://jade-testdata/encodetest/files/2018/01/18/82aab61a-1e9b-43d3-8836-d9c54cf37dd6/ENCFF538GKX.bam.bai",
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.bam", // 14
        "gs://jade-testdata/encodetest/files/2018/05/04/289b5fd2-ea5e-4275-a56d-2185738737e0/ENCFF823AJQ.bam.bai"
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
        "/encodefiles/20180118/ENCFF168GKX.bam",
        "/encodefiles/20180118/ENCFF168GKX.bam.bai",
        "/encodefiles/20180118/ENCFF538GKX.bam",
        "/encodefiles/20180118/ENCFF538GKX.bam.bai",
        "/encodefiles/20180504/ENCFF823AJQ.bam",
        "/encodefiles/20180504/ENCFF823AJQ.bam.bai"
      };
}
