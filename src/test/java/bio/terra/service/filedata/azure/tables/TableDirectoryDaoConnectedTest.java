package bio.terra.service.filedata.azure.tables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.data.tables.TableServiceClient;
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
public class TableDirectoryDaoConnectedTest {
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mrg-tdr-dev-preview-20210802154510";
  private static final String STORAGE_ACCOUNT_NAME = "tdrshiqauwlpzxavohmxxhfv";

  private UUID applicationId;
  private UUID storageAccountId;
  private UUID datasetId;

  private AzureApplicationDeploymentResource applicationResource;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;
  private TableServiceClient tableServiceClient;

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

    tableServiceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);
  }

  @After
  public void cleanup() throws Exception {

    connectedOperations.teardown();
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    // Test re-using same directory path, but for different files
    String sharedTargetPath = "/test/path/";
    String fileName1 = "file1.json";
    var fileEntry1 = createStorageTableEntrySharedBasePath(sharedTargetPath, fileName1);
    String fileName2 = "file2.json";
    var fileEntry2 = createStorageTableEntrySharedBasePath(sharedTargetPath, fileName2);

    assertThat(
        "FireStoreDirectoryEntry should now exist",
        fileEntry1.getPath(),
        equalTo(fileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName1)));
    assertThat(
        "FireStoreDirectoryEntry should now exist",
        fileEntry2.getPath(),
        equalTo(fileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName2)));
  }

  private FireStoreDirectoryEntry createStorageTableEntrySharedBasePath(
      String sharedTargetPath, String fileName) {
    UUID fileId = UUID.randomUUID();
    String loadTag = Names.randomizeName("loadTag");

    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(fileId.toString())
            .isFileRef(true)
            .path(fileMetadataUtils.getDirectoryPath(sharedTargetPath + fileName))
            .name(fileMetadataUtils.getName(sharedTargetPath + fileName))
            .datasetId(datasetId.toString())
            .loadTag(loadTag);
    tableDao.createDirectoryEntry(newEntry, billingProfile, storageAccountResource);

    // test that directory entry now exists
    return tableDirectoryDao.retrieveByPath(
        tableServiceClient, datasetId.toString(), sharedTargetPath + fileName);
  }
}
