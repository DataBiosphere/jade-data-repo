package bio.terra.service.filedata.azure.tables;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.tabulardata.azure.AzureStorageTablePdao;
import com.azure.data.tables.TableServiceClient;
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
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mrg-tdr-dev-preview-20210802154510";
  private static final String STORAGE_ACCOUNT_NAME = "tdrshiqauwlpzxavohmxxhfv";

  private UUID applicationId;
  private UUID storageAccountId;
  private UUID datasetId;

  private AzureApplicationDeploymentResource applicationResource;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;

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

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    applicationId = UUID.randomUUID();
    storageAccountId = UUID.randomUUID();
    datasetId = UUID.randomUUID();

    billingProfile =
        new BillingProfileModel()
            .id(UUID.randomUUID())
            .profileName(Names.randomizeName("somename"))
            .biller("direct")
            .billingAccountId(testConfig.getGoogleBillingAccountId())
            .description("random description")
            .cloudPlatform(CloudPlatform.AZURE)
            .tenantId(testConfig.getTargetTenantId())
            .subscriptionId(testConfig.getTargetSubscriptionId())
            .resourceGroupName(testConfig.getTargetResourceGroupName())
            .applicationDeploymentName(testConfig.getTargetApplicationName());

    applicationResource =
        new AzureApplicationDeploymentResource()
            .id(applicationId)
            .azureApplicationDeploymentName(testConfig.getTargetApplicationName())
            .azureResourceGroupName(MANAGED_RESOURCE_GROUP_NAME)
            .profileId(billingProfile.getId());
    storageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(storageAccountId)
            .name(STORAGE_ACCOUNT_NAME)
            .applicationResource(applicationResource)
            .metadataContainer("metadata")
            .dataContainer("data");
  }

  @After
  public void cleanup() throws Exception {
    // TODO - clean up the load history table
    connectedOperations.teardown();
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    String loadTag = UUID.randomUUID().toString();
    TableServiceClient serviceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);
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
    TableServiceClient serviceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);
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
