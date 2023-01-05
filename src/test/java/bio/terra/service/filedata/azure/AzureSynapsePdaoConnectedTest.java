package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.Relationship;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetRelationship;
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
import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
  private String snapshotCreateFlightId;
  private List<String> parquetFileNames;
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
  private static final String TABLE_NAME_PREFIX = "ingest_";
  private static final String SCRATCH_TABLE_NAME_PREFIX = "scratch_";

  private String snapshotQueryCredentialName;
  private String snapshotQueryDataSourceName;
  private String snapshotScopedCredentialName;
  private String snapshotDataSourceName;
  private String sourceDatasetScopedCredentialName;
  private String sourceDatasetDataSourceName;
  private String tableName;
  private String scratchTableName;
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mrg-tdr-dev-preview-20210802154510";
  private static final String STORAGE_ACCOUNT_NAME = "tdrshiqauwlpzxavohmxxhfv";
  private static final UUID snapshotId = UUID.randomUUID();

  private String snapshotStorageAccountId;
  private String datasetStorageAccountId;

  private AzureResourceManager client;
  private AzureApplicationDeploymentResource applicationResource;
  private AzureStorageAccountResource datasetStorageAccountResource;
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
  @Autowired JsonLoader jsonLoader;

  @Before
  public void setup() throws Exception {
    parquetFileNames = new ArrayList<>();
    connectedOperations.stubOutSamCalls(samService);
    randomFlightId = ShortUUID.get();
    snapshotCreateFlightId = ShortUUID.get();
    snapshotQueryDataSourceName = null;
    snapshotQueryCredentialName = null;
    tableName = TABLE_NAME_PREFIX + randomFlightId;
    scratchTableName = SCRATCH_TABLE_NAME_PREFIX + randomFlightId;
    snapshotScopedCredentialName =
        IngestUtils.getTargetScopedCredentialName(snapshotCreateFlightId);
    snapshotDataSourceName = IngestUtils.getTargetDataSourceName(snapshotCreateFlightId);
    sourceDatasetScopedCredentialName =
        IngestUtils.getSourceDatasetScopedCredentialName(snapshotCreateFlightId);
    sourceDatasetDataSourceName =
        IngestUtils.getSourceDatasetDataSourceName(snapshotCreateFlightId);
    UUID applicationId = UUID.randomUUID();

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

    //    datasetStorageAccountResource =
    //        new AzureStorageAccountResource()
    //            .resourceId(storageAccountId)
    //            .name(STORAGE_ACCOUNT_NAME)
    //            .applicationResource(applicationResource)
    //            .metadataContainer("metadata");

    client =
        azureResourceConfiguration.getClient(
            azureResourceConfiguration.getCredentials().getHomeTenantId(),
            billingProfile.getSubscriptionId());

    StorageAccount datasetStorageAccount =
        client
            .storageAccounts()
            .define("ctdataset" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(MANAGED_RESOURCE_GROUP_NAME)
            .create();
    datasetStorageAccountId = datasetStorageAccount.id();
    datasetStorageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name(datasetStorageAccount.name())
            .applicationResource(applicationResource)
            .metadataContainer("metadata");

    StorageAccount snapshotStorageAccount =
        client
            .storageAccounts()
            .define("ctsnapshot" + Instant.now().toEpochMilli())
            .withRegion(Region.US_CENTRAL)
            .withExistingResourceGroup(MANAGED_RESOURCE_GROUP_NAME)
            .create();
    snapshotStorageAccountId = snapshotStorageAccount.id();
    snapshotStorageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name(snapshotStorageAccount.name())
            .applicationResource(applicationResource)
            .metadataContainer("metadata");
  }

  @After
  public void cleanup() throws Exception {
    try {
      for (var parquetFile : parquetFileNames) {
        synapseUtils.deleteParquetFile(
            billingProfile,
            datasetStorageAccountResource,
            parquetFile,
            new BlobSasTokenOptions(
                Duration.ofMinutes(15),
                new BlobSasPermission().setReadPermission(true).setDeletePermission(true),
                null));
      }

      // check to see if successful delete
      List<String> emptyList =
          synapseUtils.readParquetFileStringColumn(
              IngestUtils.getParquetFilePath("all_data_types", randomFlightId),
              IngestUtils.getTargetDataSourceName(randomFlightId),
              "first_name",
              false);
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
            IngestUtils.formatSnapshotTableName(snapshotId, "all_data_types"),
            IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    azureSynapsePdao.dropDataSources(
        Stream.of(
                snapshotDataSourceName,
                IngestUtils.getTargetDataSourceName(randomFlightId),
                IngestUtils.getIngestRequestDataSourceName(randomFlightId),
                snapshotQueryDataSourceName)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
    azureSynapsePdao.dropScopedCredentials(
        Stream.of(
                snapshotScopedCredentialName,
                IngestUtils.getTargetScopedCredentialName(randomFlightId),
                IngestUtils.getIngestRequestScopedCredentialName(randomFlightId),
                snapshotQueryCredentialName)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));

    client.storageAccounts().deleteById(snapshotStorageAccountId);
    client.storageAccounts().deleteById(datasetStorageAccountId);
    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQueryCSV() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    testSynapseQuery(
        ingestRequestModel, "azure-simple-dataset-ingest-request.csv", SAMPLE_DATA_CSV);
  }

  @Test
  public void testSynapseQueryNonStandardCSV() throws Exception {
    IngestRequestModel nonStandardIngestRequestModel =
        new IngestRequestModel()
            .format(FormatEnum.CSV)
            .csvSkipLeadingRows(2)
            .csvFieldDelimiter("!")
            .csvQuote("*");
    // Add exclamation points to the end of the expected text fields
    var testData =
        SAMPLE_DATA_CSV.stream()
            .map(
                r ->
                    r.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
            .peek(r -> r.put("textCol", r.get("textCol").map(v -> v + "!")))
            .toList();
    testSynapseQuery(
        nonStandardIngestRequestModel,
        "azure-simple-dataset-ingest-request-non-standard.csv",
        testData);

    List<String> textCols =
        synapseUtils.readParquetFileStringColumn(
            IngestUtils.getParquetFilePath("all_data_types", randomFlightId),
            IngestUtils.getTargetDataSourceName(randomFlightId),
            "textCol",
            true);
    assertThat(
        "The text columns should be properly quoted", textCols, equalTo(List.of("Dao!", "Jones!")));
  }

  @Test
  public void testSynapseQueryJSON() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    testSynapseQuery(ingestRequestModel, "azure-ingest-request.json", SAMPLE_DATA);
  }

  @Test
  public void testSnapshotByAsset() throws SQLException, IOException {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);

    // Prep dataset data for snapshot
    Snapshot snapshot = new Snapshot().id(snapshotId);
    // Table 1 - All Data Types Table
    DatasetTable allDataTypesTable =
        ingestIntoAllDataTypesTable(ingestRequestModel, "azure-simple-dataset-ingest-request.csv");
    SnapshotTable allDataTypesSnapshotTable = setupSnapshotTable(allDataTypesTable);

    // Table 2 - date_of_birth
    String dateOfBirthTableIngestFlightId = UUID.randomUUID().toString();
    DatasetTable dateOfBirthTable =
        ingestIntoTable(
            "ingest-test-dataset-table-date-of-birth.json",
            ingestRequestModel,
            "ingest-test-dataset-date-of-birth-rows.csv",
            dateOfBirthTableIngestFlightId,
            "synapsetestdata/test",
            4);
    SnapshotTable dateOfBirthSnapshotTable = setupSnapshotTable(dateOfBirthTable);

    // Table 3 - Participant
    String participantTableIngestFlightId = UUID.randomUUID().toString();
    DatasetTable participantTable =
        ingestIntoTable(
            "ingest-test-dataset-table-participant.json",
            ingestRequestModel,
            "ingest-test-dataset-participant-rows.csv",
            participantTableIngestFlightId,
            "synapsetestdata/test",
            4);

    SnapshotTable participantSnapshotTable = setupSnapshotTable(participantTable);

    snapshot.snapshotTables(
        List.of(allDataTypesSnapshotTable, dateOfBirthSnapshotTable, participantSnapshotTable));

    // Define relationships
    AssetRelationship participantDOBRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(participantTable.getColumnByName("id"))
                    .fromTable(participantTable)
                    .toColumn(dateOfBirthTable.getColumnByName("participant_id"))
                    .toTable(dateOfBirthTable)
                    .name("participant_dob_table"));
    AssetRelationship participantADTRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(participantTable.getColumnByName("first_name"))
                    .fromTable(participantTable)
                    .toColumn(allDataTypesTable.getColumnByName("first_name"))
                    .toTable(allDataTypesTable)
                    .name("participant_allDataTypes_table"));
    List<AssetRelationship> relationships =
        List.of(participantDOBRelationship, participantADTRelationship);

    // ----- SNAPSHOT ---
    // -- CreateSnapshotSourceDatasetDataSourceAzureStep --
    // Create external data source for the source dataset
    // Where we'll pull the dataset data from to then write to the snapshot
    String parquetDatasetSourceLocation = datasetStorageAccountResource.getStorageAccountUrl();
    BlobUrlParts datasetSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDatasetSourceLocation,
            billingProfile,
            datasetStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA,
            TEST_USER);
    azureSynapsePdao.getOrCreateExternalDataSource(
        datasetSignUrlBlob, sourceDatasetScopedCredentialName, sourceDatasetDataSourceName);

    // -- CreateSnapshotTargetDataSourceAzureStep --
    // Create external data source for the snapshot
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

    // -- CreateSnapshotByAssetParquetFilesAzureStep --
    // Create snapshot parquet files via external table
    // By Asset
    String assetName = "testAsset";
    List<DatasetTable> datasetTables =
        List.of(allDataTypesTable, dateOfBirthTable, participantTable);
    String rootTableName = participantTable.getRawTableName();
    String rootColumnName = "id";
    String rootValue = "1";
    Map<String, Long> snapshotByAssetTableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByAsset(
            buildAssetSpecification(
                datasetTables, assetName, rootTableName, rootColumnName, relationships),
            snapshotId,
            sourceDatasetDataSourceName,
            snapshotDataSourceName,
            new SnapshotRequestAssetModel().assetName(assetName).addRootValuesItem(rootValue));

    String snapshotParquetFileName =
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, rootTableName);
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Sally")));
    assertThat(
        "Table row count should equal 1 for destination table",
        snapshotByAssetTableRowCounts.get(rootTableName),
        equalTo(1L));
    // TODO - When we build out the walk relationships, we should also check the other two tables
    // and make sure those rows are populated

    // 7 - Create snapshot row ids parquet file via external table
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        snapshot.getTables(), snapshotId, snapshotDataSourceName, snapshotByAssetTableRowCounts);
    String snapshotRowIdsParquetFileName =
        IngestUtils.getRetrieveSnapshotParquetFilePath(snapshotId, PDAO_ROW_ID_TABLE);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(3));

    // Updated snapshot w/ rowId
    allDataTypesSnapshotTable.rowCount(snapshotRowIds.size());
    // snapshot.snapshotTables(List.of(snapshotTable));

    //        List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    //        assertThat("2 fileRefs Returned.", refIds.size(), equalTo(2));

  }

  private AssetSpecification buildAssetSpecification(
      List<DatasetTable> datasetTables,
      String assetName,
      String rootTableName,
      String rootColumnName,
      List<AssetRelationship> relationships) {
    List<AssetTable> assetTables =
        datasetTables.stream()
            .map(
                datasetTable -> {
                  // Define AssetTable
                  List<AssetColumn> columns = new ArrayList<>();
                  datasetTable.getColumns().stream()
                      .forEach(
                          c ->
                              columns.add(
                                  new AssetColumn().datasetColumn(c).datasetTable(datasetTable)));
                  return new AssetTable().datasetTable(datasetTable).columns(columns);
                })
            .collect(Collectors.toList());

    AssetTable rootTable =
        assetTables.stream()
            .filter(at -> at.getTable().getName().equals(rootTableName))
            .findFirst()
            .orElseThrow();
    return new AssetSpecification()
        .name(assetName)
        .assetTables(assetTables)
        .assetRelationships(relationships)
        .rootTable(rootTable)
        .rootColumn(
            rootTable.getColumns().stream()
                .filter(c -> c.getDatasetColumn().getName().equals(rootColumnName))
                .findFirst()
                .orElseThrow());
  }

  private SnapshotTable setupSnapshotTable(DatasetTable datasetTable) {
    SnapshotTable snapshotTable = new SnapshotTable();
    snapshotTable.columns(datasetTable.getColumns());
    snapshotTable.id(datasetTable.getId());
    snapshotTable.name(datasetTable.getName());
    return snapshotTable;
  }

  private DatasetTable ingestIntoTable(
      String datasetTableSpecFilePath,
      IngestRequestModel ingestRequestModel,
      String ingestFileLocation,
      String flightId,
      String ingestRequestContainer, // TODO - move test data to ingestrequest folder in azure & can
      // remove this argument
      int numRowsToIngest)
      throws IOException, SQLException {
    DatasetTable destinationTable =
        jsonLoader.loadObject(datasetTableSpecFilePath, DatasetTable.class);
    destinationTable.id(UUID.randomUUID());
    synapseUtils.performIngest(
        destinationTable,
        ingestFileLocation,
        flightId,
        datasetStorageAccountResource,
        billingProfile,
        ingestRequestModel,
        numRowsToIngest,
        ingestRequestContainer);
    return destinationTable;
  }

  private DatasetTable ingestIntoAllDataTypesTable(
      IngestRequestModel ingestRequestModel, String ingestFileLocation)
      throws IOException, SQLException {

    DatasetTable destinationTable =
        ingestIntoTable(
            "ingest-test-dataset-table-all-data-types.json",
            ingestRequestModel,
            ingestFileLocation,
            randomFlightId,
            testConfig.getIngestRequestContainer(),
            2);
    jsonLoader.loadObject("ingest-test-dataset-table-all-data-types.json", DatasetTable.class);

    scratchParquetFile =
        "parquet/scratch_" + destinationTable.getName() + "/" + randomFlightId + ".parquet";
    parquetFileNames.add(scratchParquetFile);
    parquetFileNames.add(
        IngestUtils.getParquetFilePath(destinationTable.getName(), randomFlightId));

    // Check that the parquet files were successfully created.
    List<String> firstNames =
        synapseUtils.readParquetFileStringColumn(
            IngestUtils.getParquetFilePath(destinationTable.getName(), randomFlightId),
            IngestUtils.getTargetDataSourceName(randomFlightId),
            "first_name",
            true);
    assertThat(
        "List of names should equal the input", firstNames, equalTo(List.of("Bob", "Sally")));
    return destinationTable;
  }

  private void testSynapseQuery(
      IngestRequestModel ingestRequestModel,
      String ingestFileLocation,
      List<Map<String, Optional<Object>>> expectedData)
      throws Exception {
    // ---- part 1 - ingest metadata into parquet files associated with dataset
    DatasetTable destinationTable =
        ingestIntoAllDataTypesTable(ingestRequestModel, ingestFileLocation);

    // --- part 2 - create snapshot by full view of the ingested data
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
    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFiles(
            snapshot.getTables(),
            snapshotId,
            IngestUtils.getTargetDataSourceName(randomFlightId),
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
        snapshot.getTables(), snapshotId, snapshotDataSourceName, tableRowCounts);
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
    assertThat("4 fileRefs Returned.", refIds.size(), equalTo(4));

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
