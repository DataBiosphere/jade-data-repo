package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.tabulardata.azure.AzureStorageTablePdao;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
public class LoadHistoryStorageTableConnectedTest {
  private UUID datasetId;
  private TableServiceClient serviceClient;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired SynapseUtils synapseUtils;
  @Autowired AzureAuthService azureAuthService;
  @Autowired TableDirectoryDao tableDirectoryDao;
  @Autowired TableDao tableDao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired FileService fileService;
  @Autowired AzureStorageTablePdao storageTableDao;
  @Autowired AzureUtils azureUtils;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    datasetId = UUID.randomUUID();
    serviceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    testConfig.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://" + testConfig.getSourceStorageAccountName() + ".table.core.windows.net")
            .buildClient();
  }

  @After
  public void cleanup() throws Exception {
    connectedOperations.deleteLoadHistory(datasetId, serviceClient);
    connectedOperations.teardown();
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    List<BulkLoadHistoryModel> loadHistoryArray = new ArrayList<>();
    // ---- TEST FOUR STATES OF LOAD HISTORY TABLE ENTRIES ----
    // SUCCEEDED
    String checksum = Names.randomizeName("CRCSUM");
    String successfulSourcePath = "/" + UUID.randomUUID() + "/source/path.json";
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.SUCCEEDED)
            .fileId(UUID.randomUUID().toString())
            .checksumCRC(checksum)
            .checksumMD5("MD5TEST")
            .sourcePath(successfulSourcePath)
            .targetPath("/target/path.json"));
    // FAILED
    String failedSourcePath = "/" + UUID.randomUUID() + "/source/path.json";
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.FAILED)
            .sourcePath(failedSourcePath)
            .targetPath("/target/path.json"));
    // NOT_TRIED
    String notTriedSourcePath = "/" + UUID.randomUUID() + "/source/path.json";
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.NOT_TRIED)
            .sourcePath(notTriedSourcePath)
            .targetPath("/target2/path2.json"));
    // RUNNING
    String runningSourcePath = "/" + UUID.randomUUID() + "/source/path.json";
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.RUNNING)
            .sourcePath(runningSourcePath)
            .targetPath("/target3/path3.json"));

    // ---- ADD ENTRIES TO LOAD HISTORY TABLE ----
    String loadTag = UUID.randomUUID().toString();
    storageTableDao.storeLoadHistory(
        serviceClient, datasetId, loadTag, Instant.now(), loadHistoryArray);

    // --- Confirm load history table has entries ---
    List<BulkLoadHistoryModel> resultingLoadHistory =
        storageTableDao.getLoadHistory(serviceClient, datasetId, loadTag, 0, 4);

    BulkLoadHistoryModel successfulEntry =
        findEntryByState(resultingLoadHistory, BulkLoadFileState.SUCCEEDED);
    assertThat(
        "SUCCESSFUL entry is successful w/ right checksum",
        successfulEntry.getChecksumCRC(),
        equalTo(checksum));
    BulkLoadHistoryModel failedEntry =
        findEntryByState(resultingLoadHistory, BulkLoadFileState.FAILED);
    assertThat(
        "FAILED entry has correct source path",
        failedEntry.getSourcePath(),
        equalTo(failedSourcePath));

    BulkLoadHistoryModel notTriedEntry =
        findEntryByState(resultingLoadHistory, BulkLoadFileState.NOT_TRIED);
    assertThat(
        "NOTE_TRIED entry has correct source path",
        notTriedEntry.getSourcePath(),
        equalTo(notTriedSourcePath));
    BulkLoadHistoryModel runningEntry =
        findEntryByState(resultingLoadHistory, BulkLoadFileState.RUNNING);
    assertThat(
        "RUNNING entry has correct source path",
        runningEntry.getSourcePath(),
        equalTo(runningSourcePath));
  }

  private BulkLoadHistoryModel findEntryByState(
      List<BulkLoadHistoryModel> resultingLoadHistory, BulkLoadFileState state) {
    return resultingLoadHistory.stream().filter(s -> s.getState() == state).findFirst().get();
  }

  @Test
  public void testFailedFileIngestStorageTableMetadata() {
    String loadTag = UUID.randomUUID().toString();
    List<BulkLoadHistoryModel> loadHistoryArray = new ArrayList<>();
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.FAILED)
            .sourcePath("/source/path.json")
            .targetPath("/target/path.json"));

    storageTableDao.storeLoadHistory(
        serviceClient, datasetId, loadTag, Instant.now(), loadHistoryArray);
  }
}
