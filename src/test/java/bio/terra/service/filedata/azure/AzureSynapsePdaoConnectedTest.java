package bio.terra.service.filedata.azure;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.ShortUUID;
import com.azure.storage.blob.BlobUrlParts;
import java.util.Arrays;
import java.util.List;
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
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);
  private String randomFlightId;
  private DatasetSummaryModel datasetSummary;

  private static final String CONTROL_FILE_SCOPED_CREDENTIAL_PREFIX = "cfsas_";
  private static final String DESTINATION_SCOPED_CREDENTIAL_PREFIX = "dsas_";
  private static final String CONTROL_FILE_DATA_SOURCE_PREFIX = "cfds_";
  private static final String DESTINATION_DATA_SOURCE_PREFIX = "dds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";

  private String controlFileScopedCredentialName;
  private String destinationScopedCredentialName;
  private String controlFileDataSourceName;
  private String destinationDataSourceName;
  private String tableName;
  private UUID applicationId;
  private UUID storageAccountId;
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mrg-tdr-dev-preview-20210802154510";
  private static final String STORAGE_ACCOUNT_NAME = "tdrshiqauwlpzxavohmxxhfv";
  private AzureApplicationDeploymentResource applicationResource;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    randomFlightId = ShortUUID.get();
    controlFileScopedCredentialName = CONTROL_FILE_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    destinationScopedCredentialName = DESTINATION_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    controlFileDataSourceName = CONTROL_FILE_DATA_SOURCE_PREFIX + randomFlightId;
    destinationDataSourceName = DESTINATION_DATA_SOURCE_PREFIX + randomFlightId;
    tableName = TABLE_NAME_PREFIX + randomFlightId;
    applicationId = UUID.randomUUID();
    storageAccountId = UUID.randomUUID();

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
            .metadataContainer("metadata");
  }

  @After
  public void cleanup() throws Exception {
    azureSynapsePdao.dropTables(Arrays.asList(tableName));
    azureSynapsePdao.dropDataSources(
        Arrays.asList(destinationDataSourceName, controlFileDataSourceName));
    azureSynapsePdao.dropScopedCredentials(
        Arrays.asList(destinationScopedCredentialName, controlFileScopedCredentialName));

    // TODO - Clean out test parquet files

    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQueryCSV() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    String ingestFileName = "azure-simple-dataset-ingest-request.csv";
    testSynapseQuery(ingestRequestModel, ingestFileName);
  }

  @Test
  public void testSynapseQueryJSON() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    String ingestFileName = "azure-simple-dataset-ingest-request.json";
    testSynapseQuery(ingestRequestModel, ingestFileName);
  }

  private void testSynapseQuery(IngestRequestModel ingestRequestModel, String ingestFileName)
      throws Exception {
    // Currently, the parquet files will live in the same location as the ingest control file
    String destinationTableName = "participant";
    String destinationParquetFile =
        "parquet/" + destinationTableName + "/" + randomFlightId + ".parquet";

    UUID tenantId = testConfig.getTargetTenantId();

    // ---- ingest steps ---
    // 1 - Create external data source for the ingest control file
    String ingestFileLocation = "https://tdrconnectedsrc1.blob.core.windows.net/synapsetestdata";

    BlobUrlParts ingestRequestSignUrlBlob =
        azureSynapsePdao.getOrSignUrlForSourceFactory(ingestFileLocation, tenantId);
    azureSynapsePdao.createExternalDataSource(
        ingestRequestSignUrlBlob, controlFileScopedCredentialName, controlFileDataSourceName);

    // 2 - Create the external data source for the destination
    // where we'll write the resulting parquet files
    String parquetDestinationLocation =
        "https://tdrshiqauwlpzxavohmxxhfv.blob.core.windows.net/metadata";

    BlobUrlParts destinationSignUrlBlob =
        azureSynapsePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation, billingProfile, storageAccountResource);
    azureSynapsePdao.createExternalDataSource(
        destinationSignUrlBlob, destinationScopedCredentialName, destinationDataSourceName);

    // TODO - Add basic check to make sure the data source is created successfully
    // Maybe a basic query?

    // 3 - Retrieve info about database schema so that we can populate the parquet create query

    List<String> columnNames =
        Arrays.asList("id", "age", "first_name", "last_name", "favorite_animals");
    TableDataType baseType = TableDataType.STRING;
    DatasetTable destinationTable =
        DatasetFixtures.generateDatasetTable(destinationTableName, baseType, columnNames);

    // 4 - Create parquet files via external table
    azureSynapsePdao.createParquetFiles(
        ingestRequestModel.getFormat(),
        destinationTable,
        ingestFileName,
        destinationParquetFile,
        destinationDataSourceName,
        controlFileDataSourceName,
        tableName,
        ingestRequestModel.getCsvSkipLeadingRows());

    // TODO - Add check that the parquet files were successfully created.
    // How do we query the parquet files?

    // 4 - clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight
    // azureSynapsePdao.cleanSynapseEntries(randomFlightId);

  }
}
