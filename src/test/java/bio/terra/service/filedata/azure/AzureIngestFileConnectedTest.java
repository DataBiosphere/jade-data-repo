package bio.terra.service.filedata.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.AzureUtils;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.filedata.FileMetadataUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.azure.*;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
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
public class AzureIngestFileConnectedTest {
  private static final Logger logger = LoggerFactory.getLogger(AzureIngestFileConnectedTest.class);
  private UUID datasetId;
  private String targetPath;
  private UUID homeTenantId;
  private String fileId;

  private AzureStorageAuthInfo storageAuthInfo;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;
  private FileLoadModel fileLoadModel;
  private TableServiceClient tableServiceClient;

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
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
  @Autowired AzureUtils azureUtils;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    UUID applicationId = UUID.randomUUID();
    UUID storageAccountId = UUID.randomUUID();
    datasetId = UUID.randomUUID();

    homeTenantId = azureResourceConfiguration.getCredentials().getHomeTenantId();
    azureResourceConfiguration.getCredentials().setHomeTenantId(testConfig.getTargetTenantId());

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

    AzureApplicationDeploymentResource applicationResource =
        new AzureApplicationDeploymentResource()
            .id(applicationId)
            .azureApplicationDeploymentName(testConfig.getTargetApplicationName())
            .azureResourceGroupName(testConfig.getTargetResourceGroupName())
            .profileId(billingProfile.getId());

    storageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(storageAccountId)
            .name(testConfig.getSourceStorageAccountName())
            .applicationResource(applicationResource)
            .metadataContainer("metadata")
            .dataContainer("data");

    storageAuthInfo =
        AzureStorageAuthInfo.azureStorageAuthInfoBuilder(billingProfile, storageAccountResource);

    tableServiceClient =
        new TableServiceClientBuilder()
            .credential(
                new AzureNamedKeyCredential(
                    testConfig.getSourceStorageAccountName(),
                    azureUtils.getSourceStorageAccountPrimarySharedKey()))
            .endpoint(
                "https://" + testConfig.getSourceStorageAccountName() + ".table.core.windows.net")
            .buildClient();

    String exampleAzureFileToIngest =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-simple-dataset-ingest-request.json");

    targetPath = "/test/path/file.json";
    fileLoadModel =
        new FileLoadModel()
            .sourcePath(exampleAzureFileToIngest)
            .profileId(billingProfile.getId())
            .description("test example file azure load")
            .mimeType("application/json")
            .targetPath(targetPath)
            .loadTag(Names.randomizeName("loadTag"));

    fileId = UUID.randomUUID().toString();
  }

  @After
  public void cleanup() throws Exception {
    try {
      tableDao.deleteFileMetadata(fileId, storageAuthInfo);
      tableDirectoryDao.deleteDirectoryEntry(tableServiceClient, fileId);
    } catch (Exception ex) {
      logger.error("Unable to clean up metadata for fileId {}", fileId, ex);
    }
    azureResourceConfiguration.getCredentials().setHomeTenantId(homeTenantId);
    connectedOperations.teardown();
  }

  @Test
  public void testStorageTableMetadataDuringFileIngest() {
    // 0 - IngestFileIdStep - Done in test setup
    // 1 - IngestFileAzurePrimaryDataLocationStep
    // define storage account (this is already defined in the test setup)
    // 2 - IngestFileAzureMakeStorageAccountLinkStep
    // make database entry to link storage account and storage account

    // 3 - ValidateIngestFileAzureDirectoryStep
    // Check if directory already exists - it should not yet

    FireStoreDirectoryEntry de =
        tableDirectoryDao.retrieveByPath(tableServiceClient, datasetId.toString(), targetPath);
    assertThat("directory should not yet exist.", de, equalTo(null));

    // 4 - IngestFileAzureDirectoryStep
    // Testing case 1 - Directory entry doesn't exist and needs to be created
    // (1) Not there - create it
    FireStoreDirectoryEntry newEntry =
        new FireStoreDirectoryEntry()
            .fileId(fileId)
            .isFileRef(true)
            .path(fileMetadataUtils.getDirectoryPath(targetPath))
            .name(fileMetadataUtils.getName(targetPath))
            .datasetId(datasetId.toString())
            .loadTag(fileLoadModel.getLoadTag());
    tableDirectoryDao.createDirectoryEntry(tableServiceClient, newEntry);

    // test that directory entry now exists
    FireStoreDirectoryEntry de_after =
        tableDirectoryDao.retrieveByPath(tableServiceClient, datasetId.toString(), targetPath);
    assertThat("FireStoreDirectoryEntry should now exist", de_after, equalTo(newEntry));

    // 5 - IngestFileAzurePrimaryDataStep
    FSFileInfo fsFileInfo =
        azureBlobStorePdao.copyFile(billingProfile, fileLoadModel, fileId, storageAccountResource);

    // 6 - IngestFileAzureFileStep
    FireStoreFile newFile =
        new FireStoreFile()
            .fileId(fileId)
            .mimeType(fileLoadModel.getMimeType())
            .description(fileLoadModel.getDescription())
            .bucketResourceId(fsFileInfo.getBucketResourceId())
            .fileCreatedDate(fsFileInfo.getCreatedDate())
            .gspath(fsFileInfo.getCloudPath())
            .checksumCrc32c(fsFileInfo.getChecksumCrc32c())
            .checksumMd5(fsFileInfo.getChecksumMd5())
            .size(fsFileInfo.getSize())
            .loadTag(fileLoadModel.getLoadTag());

    tableDao.createFileMetadata(newFile, storageAuthInfo);
    // Retrieve to build the complete FSItem
    FSItem fsItem = tableDao.retrieveById(datasetId, fileId, 1, storageAuthInfo);
    FileModel resultingFileModel = fileService.fileModelFromFSItem(fsItem);
    assertThat(
        "file model contains correct info.", resultingFileModel.getFileId(), equalTo(fileId));

    // Testing other cases from IngestFileAzureDirectoryStep (step 4 above)
    // We use this to check if we are in re-run of a load job
    FireStoreFile fileEntry = tableDao.lookupFile(fileId, storageAuthInfo);
    assertThat("FileEntry should not be null", fileEntry, notNullValue());
  }
}
