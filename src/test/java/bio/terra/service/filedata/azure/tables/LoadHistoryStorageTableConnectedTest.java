package bio.terra.service.filedata.azure.tables;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.iam.IamProviderInterface;
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
  @Autowired FileMetadataUtils fileMetadataUtils;
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
    // TODO - clean up the load history table
    connectedOperations.teardown();
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    String loadTag = UUID.randomUUID().toString();
    List<BulkLoadHistoryModel> loadHistoryArray = new ArrayList<>();
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.SUCCEEDED)
            .fileId(UUID.randomUUID().toString())
            .checksumCRC("CRCTEST")
            .checksumMD5("MD5TEST")
            .sourcePath("/source/path.json")
            .targetPath("/target/path.json"));
    loadHistoryArray.add(
        new BulkLoadHistoryModel()
            .state(BulkLoadFileState.FAILED)
            .sourcePath("/source/path.json")
            .targetPath("/target/path.json"));

    // TODO Test the other two states

    storageTableDao.storeLoadHistory(
        serviceClient, datasetId, loadTag, Instant.now(), loadHistoryArray);

    // TODO - confirm the results

    //    List<BulkLoadHistoryModel> resultingLoadHistory =
    // storageTableDao.getLoadHistory(serviceClient, datasetId, loadTag, 0, 2);
    //    resultingLoadHistory.get(0
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
