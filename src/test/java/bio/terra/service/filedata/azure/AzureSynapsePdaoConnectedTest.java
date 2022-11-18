package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TableDataType;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.DrsIdService;
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
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  private String scratchParquetFile;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  // Represents the data that lives in Azure storage account
  private static final List<Map<String, Optional<Object>>> SAMPLE_DATA =
      List.of(
          Stream.of(
                  Map.entry("boolCol", true),
                  Map.entry("dateCol", Date.valueOf("2021-08-01")),
                  Map.entry("dateTimeCol", Timestamp.valueOf("2021-08-01 23:59:59.999999")),
                  Map.entry("dirRefCol", UUID.fromString("7a1e4648-fb95-11eb-9a03-0242ac130003")),
                  Map.entry("file", UUID.fromString("816ca5ca-fb95-11eb-9a03-0242ac130003")),
                  Map.entry("float64Col", 1.79E+308D),
                  Map.entry("floatCol", -1.18E-38F),
                  Map.entry("intCol", 2147483647),
                  Map.entry("int64Col", 9223372036854775806L),
                  Map.entry("numericCol", 12345.12000F),
                  Map.entry("first_name", "Bob"),
                  Map.entry("textCol", "Dao"),
                  Map.entry("timeCol", Time.valueOf("01:01:00")),
                  Map.entry("timestampCol", Timestamp.valueOf("2021-08-01 23:59:59.999999")),
                  Map.entry("arrayCol", List.of("lion", "tiger")))
              .collect(Collectors.toMap(Entry::getKey, e -> Optional.of(e.getValue()))),
          Stream.of(
                  Map.entry("boolCol", false),
                  Map.entry("dateCol", Date.valueOf("2021-01-01")),
                  Map.entry("dateTimeCol", Timestamp.valueOf("2021-08-01 23:59:59.999999")),
                  Map.entry("dirRefCol", UUID.fromString("856d0926-fb95-11eb-9a03-0242ac130003")),
                  Map.entry("file", UUID.fromString("89875e76-fb95-11eb-9a03-0242ac130003")),
                  Map.entry("float64Col", -1.79E+308D),
                  Map.entry("floatCol", 3.40E+38F),
                  Map.entry("intCol", -2147483647),
                  Map.entry("int64Col", -9223372036L),
                  Map.entry("numericCol", 12345.12000F),
                  Map.entry("first_name", "Sally"),
                  Map.entry("textCol", "Jones"),
                  Map.entry("timeCol", Time.valueOf("01:01:00")),
                  Map.entry("timestampCol", Timestamp.valueOf("2021-08-01 23:59:59.999999")),
                  Map.entry("arrayCol", List.of("horse", "dog")))
              .collect(Collectors.toMap(Entry::getKey, e -> Optional.of(e.getValue()))));
  private static final List<Map<String, Optional<Object>>> SAMPLE_DATA_CSV =
      SAMPLE_DATA.stream()
          // Copy the records so that changing them in this list doesn't affect the original
          .map(r -> r.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
          // We can't load array data with CSVs
          .peek(r -> r.put("arrayCol", Optional.empty()))
          .toList();
  private static final String INGEST_REQUEST_SCOPED_CREDENTIAL_PREFIX = "irsas_";
  private static final String DESTINATION_SCOPED_CREDENTIAL_PREFIX = "dsas_";
  private static final String SNAPSHOT_SCOPED_CREDENTIAL_PREFIX = "ssas_";
  private static final String INGEST_REQUEST_DATA_SOURCE_PREFIX = "irds_";
  private static final String DESTINATION_DATA_SOURCE_PREFIX = "dds_";
  private static final String SNAPSHOT_DATA_SOURCE_PREFIX = "sds_";
  private static final String TABLE_NAME_PREFIX = "ingest_";
  private static final String SCRATCH_TABLE_NAME_PREFIX = "scratch_";

  private String ingestRequestScopedCredentialName;
  private String destinationScopedCredentialName;
  private String snapshotScopedCredentialName;
  private String snapshotQueryCredentialName;
  private String ingestRequestDataSourceName;
  private String destinationDataSourceName;
  private String snapshotDataSourceName;
  private String snapshotQueryDataSourceName;
  private String tableName;
  private String scratchTableName;
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
    snapshotQueryDataSourceName = null;
    snapshotQueryCredentialName = null;
    tableName = TABLE_NAME_PREFIX + randomFlightId;
    scratchTableName = SCRATCH_TABLE_NAME_PREFIX + randomFlightId;
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
      for (var parquetFile : List.of(scratchParquetFile, destinationParquetFile)) {
        synapseUtils.deleteParquetFile(
            billingProfile,
            storageAccountResource,
            parquetFile,
            new BlobSasTokenOptions(
                Duration.ofMinutes(15),
                new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
                null));
      }

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
            scratchTableName,
            tableName,
            IngestUtils.formatSnapshotTableName(snapshotId, "participant"),
            IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    azureSynapsePdao.dropDataSources(
        Stream.of(
                snapshotDataSourceName,
                destinationDataSourceName,
                ingestRequestDataSourceName,
                snapshotQueryDataSourceName)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
    azureSynapsePdao.dropScopedCredentials(
        Stream.of(
                snapshotScopedCredentialName,
                destinationScopedCredentialName,
                ingestRequestScopedCredentialName,
                snapshotQueryCredentialName)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));

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
    runIngest(ingestRequestModel, ingestFileLocation);
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
    // Add exclamation points to the end of the expected text fields
    var testData =
        SAMPLE_DATA_CSV.stream()
            .map(
                r ->
                    r.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
            .peek(r -> r.put("textCol", r.get("textCol").map(v -> v + "!")))
            .toList();
    runIngest(nonStandardIngestRequestModel, nonStandardIngestFileLocation);

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
    runIngest(ingestRequestModel, ingestFileLocation);
  }

  @Test
  public void testSnapshotByAsset() throws SQLException {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-simple-dataset-ingest-request.csv");
    List<Map<String, Optional<Object>>> expectedData = SAMPLE_DATA_CSV;

    DatasetTable destinationTable = runIngest(ingestRequestModel, ingestFileLocation);

    // SNAPSHOT
    Snapshot snapshot = new Snapshot().id(snapshotId);
    SnapshotTable snapshotTable = new SnapshotTable();
    snapshotTable.columns(destinationTable.getColumns());
    snapshotTable.id(destinationTable.getId());
    snapshotTable.name(destinationTable.getName());
    snapshot.snapshotTables(List.of(snapshotTable));

    // 5 - Create external data source for the snapshot

    // where we'll write the resulting parquet files
    String parquetSnapshotLocation = snapshotStorageAccountResource.getStorageAccountUrl();
    BlobUrlParts snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetSnapshotLocation,
            billingProfile,
            snapshotStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        snapshotSignUrlBlob, snapshotScopedCredentialName, snapshotDataSourceName);

    // 6 - Create snapshot parquet files via external table
    // By Asset
    String assetName = "testAsset";
    Map<String, Long> snapshotByAssetTableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByAsset(
            buildAssetSpecification(destinationTable, assetName),
            snapshotId,
            destinationDataSourceName,
            snapshotDataSourceName,
            randomFlightId,
            new SnapshotRequestAssetModel().assetName(assetName).addRootValuesItem("Jones"));

    String snapshotParquetFileName =
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, destinationTable.getName());
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Sally")));
    assertThat(
        "Table row count should equal 1 for destination table",
        snapshotByAssetTableRowCounts.get(destinationTable.getName()),
        equalTo(1L));

    // 7 - Create snapshot row ids parquet file via external table
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        snapshot.getTables(),
        snapshotId,
        destinationDataSourceName,
        snapshotDataSourceName,
        snapshotByAssetTableRowCounts,
        randomFlightId);
    String snapshotRowIdsParquetFileName =
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, PDAO_ROW_ID_TABLE);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(1));

    // Updated snapshot w/ rowId
    snapshotTable.rowCount(snapshotRowIds.size());
    snapshot.snapshotTables(List.of(snapshotTable));

    List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    assertThat("2 fileRefs Returned.", refIds.size(), equalTo(2));

    // 12 - clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight
  }

  @Test
  public void testSnapshotByFullView() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "azure-simple-dataset-ingest-request.csv");
    List<Map<String, Optional<Object>>> expectedData = SAMPLE_DATA_CSV;

    DatasetTable destinationTable = runIngest(ingestRequestModel, ingestFileLocation);

    // SNAPSHOT
    Snapshot snapshot = new Snapshot().id(snapshotId);
    SnapshotTable snapshotTable = new SnapshotTable();
    snapshotTable.columns(destinationTable.getColumns());
    snapshotTable.id(destinationTable.getId());
    snapshotTable.name(destinationTable.getName());
    snapshot.snapshotTables(List.of(snapshotTable));

    // 5 - Create external data source for the snapshot

    // where we'll write the resulting parquet files
    String parquetSnapshotLocation = snapshotStorageAccountResource.getStorageAccountUrl();
    BlobUrlParts snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetSnapshotLocation,
            billingProfile,
            snapshotStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        snapshotSignUrlBlob, snapshotScopedCredentialName, snapshotDataSourceName);

    // 6 - Create snapshot parquet files via external table
    // By Full View
    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFiles(
            snapshot.getTables(),
            snapshotId,
            destinationDataSourceName,
            snapshotDataSourceName,
            randomFlightId);
    String snapshotParquetFileName =
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, destinationTable.getName());
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Bob", "Sally")));
    assertThat(
        "Table row count should equal 2 for destination table",
        tableRowCounts.get(destinationTable.getName()),
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
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, PDAO_ROW_ID_TABLE);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(2));

    // Updated snapshot w/ rowId
    snapshotTable.rowCount(snapshotRowIds.size());
    snapshot.snapshotTables(List.of(snapshotTable));

    List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    assertThat("2 fileRefs Returned.", refIds.size(), equalTo(4));

    // 9 - do a basic query of the data
    snapshotQueryCredentialName =
        AzureSynapsePdao.getCredentialName(snapshot, TEST_USER.getEmail());
    snapshotQueryDataSourceName =
        AzureSynapsePdao.getDataSourceName(snapshot, TEST_USER.getEmail());
    azureSynapsePdao.getOrCreateExternalDataSource(
        snapshotSignUrlBlob, snapshotQueryCredentialName, snapshotQueryDataSourceName);
    List<Map<String, Optional<Object>>> tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getSnapshotTableData(
                TEST_USER,
                snapshot,
                snapshotTable.getName(),
                10,
                0,
                "first_name",
                SqlSortDirection.ASC,
                ""));
    assertThat(
        "table query contains correct data in the right order (ascending by first name)",
        tableData,
        contains(expectedData.get(0), expectedData.get(1)));

    // 10 - now swap the order
    tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getSnapshotTableData(
                TEST_USER,
                snapshot,
                snapshotTable.getName(),
                10,
                0,
                "first_name",
                SqlSortDirection.DESC,
                ""));
    assertThat(
        "table query contains correct data in the right order (descending by first name)",
        tableData,
        contains(expectedData.get(1), expectedData.get(0)));

    // 11 - now read a single value
    tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getSnapshotTableData(
                TEST_USER,
                snapshot,
                snapshotTable.getName(),
                10,
                0,
                "first_name",
                SqlSortDirection.ASC,
                "upper(first_name)='SALLY'"));
    assertThat(
        "table query contains only a single record", tableData, contains(expectedData.get(1)));

    // 12 - clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight

  }

  private DatasetTable runIngest(IngestRequestModel ingestRequestModel, String ingestFileLocation)
      throws SQLException {
    UUID tenantId = testConfig.getTargetTenantId();

    // ---- ingest steps ---
    // 0 -
    // A - Collect user input and validate
    IngestUtils.validateBlobAzureBlobFileURL(ingestFileLocation);
    String destinationTableName = "participant";

    // B - Build parameters based on user input
    destinationParquetFile = "parquet/" + destinationTableName + "/" + randomFlightId + ".parquet";

    scratchParquetFile =
        "parquet/scratch_" + destinationTableName + "/" + randomFlightId + ".parquet";

    // 1 - Create external data source for the ingest control file
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(ingestFileLocation, tenantId, TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        ingestRequestSignUrlBlob, ingestRequestScopedCredentialName, ingestRequestDataSourceName);

    // 2 - Create the external data source for the destination
    // where we'll write the resulting parquet files
    // We will build this parquetDestinationLocation according
    // to the associated storage account for the dataset
    String parquetDestinationLocation = storageAccountResource.getStorageAccountUrl();

    BlobUrlParts destinationSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation,
            billingProfile,
            storageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        destinationSignUrlBlob, destinationScopedCredentialName, destinationDataSourceName);

    // 3 - Retrieve info about database schema so that we can populate the parquet create query
    DatasetTable destinationTable = buildExampleTableSchema(destinationTableName);

    // 4 - Create parquet files via external table
    // All inputs should be sanitized before passed into this method
    int updateCount =
        azureSynapsePdao.createScratchParquetFiles(
            ingestRequestModel.getFormat(),
            destinationTable,
            ingestRequestSignUrlBlob.getBlobName(),
            scratchParquetFile,
            destinationDataSourceName,
            ingestRequestDataSourceName,
            scratchTableName,
            ingestRequestModel.getCsvSkipLeadingRows(),
            ingestRequestModel.getCsvFieldDelimiter(),
            ingestRequestModel.getCsvQuote());
    assertThat("num rows updated is two", updateCount, equalTo(2));

    int failedRows =
        azureSynapsePdao.validateScratchParquetFiles(destinationTable, scratchTableName);

    assertThat("there are no rows that fail validation", failedRows, equalTo(0));

    azureSynapsePdao.createFinalParquetFiles(
        tableName,
        destinationParquetFile,
        destinationDataSourceName,
        scratchTableName,
        destinationTable);

    // Check that the parquet files were successfully created.
    List<String> firstNames =
        synapseUtils.readParquetFileStringColumn(
            destinationParquetFile, destinationDataSourceName, "first_name", true);
    assertThat(
        "List of names should equal the input", firstNames, equalTo(List.of("Bob", "Sally")));
    return destinationTable;
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

  // TODO - have this input list of DatasetTables and then only set one as root table
  private AssetSpecification buildAssetSpecification(DatasetTable datasetTable, String assetName) {
    // Define AssetTable
    List<AssetColumn> columns = new ArrayList<>();
    datasetTable.getColumns().stream()
        .forEach(c -> columns.add(new AssetColumn().datasetColumn(c).datasetTable(datasetTable)));
    AssetTable assetTable = new AssetTable().datasetTable(datasetTable).columns(columns);

    return new AssetSpecification()
        .name(assetName)
        .assetTables(List.of(assetTable))
        .rootTable(assetTable)
        .rootColumn(
            assetTable.getColumns().stream()
                .filter(c -> c.getDatasetColumn().getName().equals("textCol"))
                .findFirst()
                .orElseThrow());
  }

  private Optional<Object> extractFileId(Optional<Object> drsUri) {
    return drsUri.map(d -> UUID.fromString(DrsIdService.fromUri(d.toString()).getFsObjectId()));
  }

  private List<Map<String, Optional<Object>>> prepQueryResultForComparison(
      List<Map<String, Optional<Object>>> tableData) {
    return tableData.stream()
        // Remove datarepo_row_id since it's random
        .peek(r -> r.remove(PDAO_ROW_ID_COLUMN))
        // Replace the DRS id with its file ID for easier comparison
        .peek(r -> r.put("file", extractFileId(r.get("file"))))
        .peek(r -> r.put("dirRefCol", extractFileId(r.get("dirRefCol"))))
        .toList();
  }
}
