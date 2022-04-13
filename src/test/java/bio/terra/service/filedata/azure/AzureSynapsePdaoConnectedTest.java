package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.fixtures.Names;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.TableDataType;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.ShortUUID;
import com.azure.core.management.Region;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.storage.models.StorageAccount;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.sas.BlobSasPermission;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class AzureSynapsePdaoConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);

  private String randomFlightId;
  private String destinationParquetFile;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final String INGEST_REQUEST_SCOPED_CREDENTIAL_PREFIX = "irsas_";
  private static final String DESTINATION_SCOPED_CREDENTIAL_PREFIX = "dsas_";
  private static final String SNAPSHOT_SCOPED_CREDENTIAL_PREFIX = "ssas_";
  private static final String INGEST_REQUEST_DATA_SOURCE_PREFIX = "irds_";
  private static final String DESTINATION_DATA_SOURCE_PREFIX = "dds_";
  private static final String SNAPSHOT_DATA_SOURCE_PREFIX = "sds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";

  private String ingestRequestScopedCredentialName;
  private String destinationScopedCredentialName;
  private String snapshotScopedCredentialName;
  private String ingestRequestDataSourceName;
  private String destinationDataSourceName;
  private String snapshotDataSourceName;
  private String tableName;
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mrg-tdr-dev-preview-20210802154510";
  private static final String STORAGE_ACCOUNT_NAME = "tdrshiqauwlpzxavohmxxhfv";
  private static final UUID snapshotId = UUID.randomUUID();
  private String snapshotStorageAccountId;

  private AzureResourceManager client;
  private AzureApplicationDeploymentResource applicationResource;
  private AzureStorageAccountResource storageAccountResource;
  private AzureStorageAccountResource snapshotStorageAccountResource;
  private BillingProfileModel billingProfile;

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;
  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired SynapseUtils synapseUtils;
  @Autowired SnapshotDao snapshotDao;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    randomFlightId = ShortUUID.get();
    ingestRequestScopedCredentialName = INGEST_REQUEST_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    destinationScopedCredentialName = DESTINATION_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    snapshotScopedCredentialName = SNAPSHOT_SCOPED_CREDENTIAL_PREFIX + randomFlightId;
    ingestRequestDataSourceName = INGEST_REQUEST_DATA_SOURCE_PREFIX + randomFlightId;
    destinationDataSourceName = DESTINATION_DATA_SOURCE_PREFIX + randomFlightId;
    snapshotDataSourceName = SNAPSHOT_DATA_SOURCE_PREFIX + randomFlightId;
    tableName = TABLE_NAME_PREFIX + randomFlightId;
    UUID applicationId = UUID.randomUUID();
    UUID storageAccountId = UUID.randomUUID();

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

    client =
        azureResourceConfiguration.getClient(
            azureResourceConfiguration.getCredentials().getHomeTenantId(),
            billingProfile.getSubscriptionId());

    StorageAccount storageAccount =
        client
            .storageAccounts()
            .define("ct" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(MANAGED_RESOURCE_GROUP_NAME)
            .create();
    snapshotStorageAccountId = storageAccount.id();
    snapshotStorageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name(storageAccount.name())
            .applicationResource(applicationResource)
            .metadataContainer("metadata");
  }

  @After
  public void cleanup() throws Exception {
    try {
      synapseUtils.deleteParquetFile(
          billingProfile,
          storageAccountResource,
          destinationParquetFile,
          new BlobSasTokenOptions(
              Duration.ofMinutes(15),
              new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
              null));

      // check to see if successful delete
      List<String> emptyList =
          synapseUtils.readParquetFileStringColumn(
              destinationParquetFile, destinationDataSourceName, "first_name", false);
      assertThat(
          "No longer able to read parquet file because it should have been delete",
          emptyList.size(),
          equalTo(0));

    } catch (Exception ex) {
      logger.info("Unable to delete parquet files.");
    }

    azureSynapsePdao.dropTables(
        List.of(
            tableName,
            IngestUtils.formatSnapshotTableName(snapshotId, "participant"),
            IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    azureSynapsePdao.dropDataSources(
        List.of(snapshotDataSourceName, destinationDataSourceName, ingestRequestDataSourceName));
    azureSynapsePdao.dropScopedCredentials(
        List.of(
            snapshotScopedCredentialName,
            destinationScopedCredentialName,
            ingestRequestScopedCredentialName));

    client.storageAccounts().deleteById(snapshotStorageAccountId);
    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQueryCSV() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-simple-dataset-ingest-request.csv");
    testSynapseQuery(ingestRequestModel, ingestFileLocation);
  }

  @Test
  public void testSynapseQueryNonStandardCSV() throws Exception {
    IngestRequestModel nonStandardIngestRequestModel =
        new IngestRequestModel()
            .format(FormatEnum.CSV)
            .csvSkipLeadingRows(2)
            .csvFieldDelimiter("!")
            .csvQuote("*");
    String nonStandardIngestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-simple-dataset-ingest-request-non-standard.csv");
    testSynapseQuery(nonStandardIngestRequestModel, nonStandardIngestFileLocation);

    List<String> textCols =
        synapseUtils.readParquetFileStringColumn(
            destinationParquetFile, destinationDataSourceName, "textCol", true);
    assertThat(
        "The text columns should be properly quoted", textCols, equalTo(List.of("Dao!", "Jones!")));
  }

  @Test
  public void testSynapseQueryJSON() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-ingest-request.json");
    testSynapseQuery(ingestRequestModel, ingestFileLocation);
  }

  private void testSynapseQuery(IngestRequestModel ingestRequestModel, String ingestFileLocation)
      throws Exception {

    UUID tenantId = testConfig.getTargetTenantId();

    // ---- ingest steps ---
    // 0 -
    // A - Collect user input and validate
    IngestUtils.validateBlobAzureBlobFileURL(ingestFileLocation);
    String destinationTableName = "participant";

    // B - Build parameters based on user input
    destinationParquetFile = "parquet/" + destinationTableName + "/" + randomFlightId + ".parquet";

    // 1 - Create external data source for the ingest control file
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(ingestFileLocation, tenantId, TEST_USER);
    azureSynapsePdao.createExternalDataSource(
        ingestRequestSignUrlBlob, ingestRequestScopedCredentialName, ingestRequestDataSourceName);

    // 2 - Create the external data source for the destination
    // where we'll write the resulting parquet files
    // We will build this parquetDestinationLocation according
    // to the associated storage account for the dataset
    String parquetDestinationLocation =
        IngestUtils.getParquetTargetLocationURL(storageAccountResource);

    BlobUrlParts destinationSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation,
            billingProfile,
            storageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.createExternalDataSource(
        destinationSignUrlBlob, destinationScopedCredentialName, destinationDataSourceName);

    // 3 - Retrieve info about database schema so that we can populate the parquet create query
    DatasetTable destinationTable = buildExampleTableSchema(destinationTableName);

    // 4 - Create parquet files via external table
    // All inputs should be sanitized before passed into this method
    int updateCount =
        azureSynapsePdao.createParquetFiles(
            ingestRequestModel.getFormat(),
            destinationTable,
            ingestRequestSignUrlBlob.getBlobName(),
            destinationParquetFile,
            destinationDataSourceName,
            ingestRequestDataSourceName,
            tableName,
            ingestRequestModel.getCsvSkipLeadingRows(),
            ingestRequestModel.getCsvFieldDelimiter(),
            ingestRequestModel.getCsvQuote());
    assertThat("num rows updated is two", updateCount, equalTo(2));

    // Check that the parquet files were successfully created.
    List<String> firstNames =
        synapseUtils.readParquetFileStringColumn(
            destinationParquetFile, destinationDataSourceName, "first_name", true);
    assertThat(
        "List of names should equal the input", firstNames, equalTo(List.of("Bob", "Sally")));

    // SNAPSHOT
    Snapshot snapshot = new Snapshot().id(snapshotId);
    SnapshotTable snapshotTable = new SnapshotTable();
    snapshotTable.columns(destinationTable.getColumns());
    snapshotTable.id(destinationTable.getId());
    snapshotTable.name(destinationTable.getName());
    snapshot.snapshotTables(List.of(snapshotTable));

    // 5 - Create external data source for the snapshot

    // where we'll write the resulting parquet files
    String parquetSnapshotLocation =
        IngestUtils.getParquetTargetLocationURL(snapshotStorageAccountResource);
    BlobUrlParts snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetSnapshotLocation,
            billingProfile,
            snapshotStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.createExternalDataSource(
        snapshotSignUrlBlob, snapshotScopedCredentialName, snapshotDataSourceName);

    // 6 - Create snapshot parquet files via external table
    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFiles(
            snapshot.getTables(),
            snapshotId,
            destinationDataSourceName,
            snapshotDataSourceName,
            randomFlightId);
    String snapshotParquetFileName =
        IngestUtils.getSnapshotParquetFilePath(snapshotId, destinationTable.getName());
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Bob", "Sally")));
    assertThat(
        "Table row count should equal 2 for destination table",
        tableRowCounts.get(destinationTableName),
        equalTo(2L));

    // 7 - Create snapshot row ids parquet file via external table
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        snapshot.getTables(),
        snapshotId,
        destinationDataSourceName,
        snapshotDataSourceName,
        tableRowCounts,
        randomFlightId);
    String snapshotRowIdsParquetFileName =
        IngestUtils.getSnapshotParquetFilePath(snapshotId, PDAO_ROW_ID_TABLE);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(2));

    // Updated snapshot w/ rowId
    snapshotTable.rowCount(snapshotRowIds.size());
    snapshot.snapshotTables(List.of(snapshotTable));

    List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    assertThat("4 fileRefs Returned.", refIds.size(), equalTo(4));

    // 4 - clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight

  }

  private DatasetTable buildExampleTableSchema(String destinationTableName) {
    List<String> columnNames =
        Arrays.asList(
            "boolCol",
            "dateCol",
            "dateTimeCol",
            "dirRefCol",
            "file", // test use of reserved word
            "floatCol",
            "float64Col",
            "intCol",
            "int64Col",
            "numericCol",
            "first_name",
            "textCol",
            "timeCol",
            "timestampCol",
            "arrayCol");
    TableDataType baseType = TableDataType.STRING;
    DatasetTable destinationTable =
        DatasetFixtures.generateDatasetTable(destinationTableName, baseType, columnNames);

    // Set each column to be a different data type so we can test them all
    List<TableDataType> tableTypesToTry =
        Arrays.asList(
            TableDataType.BOOLEAN,
            TableDataType.DATE,
            TableDataType.DATETIME,
            TableDataType.DIRREF,
            TableDataType.FILEREF,
            TableDataType.FLOAT,
            TableDataType.FLOAT64,
            TableDataType.INTEGER,
            TableDataType.INT64,
            TableDataType.NUMERIC,
            TableDataType.STRING,
            TableDataType.TEXT,
            TableDataType.TIME,
            TableDataType.TIMESTAMP,
            TableDataType.STRING);
    Iterator<TableDataType> dataTypes = tableTypesToTry.iterator();
    destinationTable.getColumns().forEach(c -> c.type(dataTypes.next()));
    destinationTable.getColumns().get(tableTypesToTry.size() - 1).arrayOf(true);

    return destinationTable;
  }
}
