package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.DrsId;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotTable;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class AzureSynapsePdaoConnectedTest {
  private String randomFlightId;
  private BlobUrlParts snapshotSignUrlBlob;
  private String snapshotQueryCredentialName;
  private String snapshotQueryDataSourceName;

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

  private String snapshotDataSourceName;
  private String sourceDatasetDataSourceName;
  private UUID snapshotId;

  private AzureStorageAccountResource datasetStorageAccountResource;
  private AzureStorageAccountResource snapshotStorageAccountResource;
  private BillingProfileModel billingProfile;

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired SynapseUtils synapseUtils;
  @Autowired SnapshotDao snapshotDao;
  @Autowired JsonLoader jsonLoader;

  @Before
  public void setup() throws Exception {
    snapshotId = UUID.randomUUID();
    synapseUtils.synapseTestSetup();
    connectedOperations.stubOutSamCalls(samService);

    // retrieve things setup in SynapseUtils
    datasetStorageAccountResource = synapseUtils.retrieveDatasetStorageAccountResource();
    billingProfile = synapseUtils.retrieveBillingProfileModel();
    randomFlightId = synapseUtils.retrieveRandomFlightId();
    snapshotStorageAccountResource = synapseUtils.retrieveSnapshotStorageAccountResource();
    sourceDatasetDataSourceName = synapseUtils.retrieveSourceDatasetDataSourceName();
    snapshotDataSourceName = synapseUtils.retrieveSnapshotDataSourceName();
    snapshotSignUrlBlob = synapseUtils.retrieveSnapshotSignUrlBlob();
  }

  @After
  public void cleanup() throws Exception {
    synapseUtils.synapseTestCleanup();
    connectedOperations.teardown();
  }

  @Test
  public void testSynapseQueryCSV() throws Exception {
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    testSynapseQuery(
        ingestRequestModel, "azure-simple-dataset-ingest-request.csv", SAMPLE_DATA_CSV, false);
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
        testData,
        false);

    List<String> textCols =
        synapseUtils.readParquetFileStringColumn(
            FolderType.METADATA.getPath(
                IngestUtils.getParquetFilePath("all_data_types", randomFlightId)),
            IngestUtils.getTargetDataSourceName(randomFlightId),
            "textCol",
            true);
    synapseUtils.addDataSource(IngestUtils.getTargetDataSourceName(randomFlightId));
    assertThat(
        "The text columns should be properly quoted", textCols, equalTo(List.of("Dao!", "Jones!")));
  }

  @Test
  public void testSynapseQueryJSON() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    testSynapseQuery(ingestRequestModel, "azure-ingest-request.json", SAMPLE_DATA, false);
  }

  @Test
  public void testSynapseQueryJSONWithGlobalFileIds() throws Exception {
    IngestRequestModel ingestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
    testSynapseQuery(ingestRequestModel, "azure-ingest-request.json", SAMPLE_DATA, true);
  }

  private void testSynapseQuery(
      IngestRequestModel ingestRequestModel,
      String ingestFileLocation,
      List<Map<String, Optional<Object>>> expectedData,
      boolean isGlobalFileIds)
      throws Exception {
    // ---- part 1 - ingest metadata into parquet files associated with dataset
    DatasetTable destinationTable =
        synapseUtils.ingestIntoAllDataTypesTable(
            ingestRequestModel,
            ingestFileLocation,
            randomFlightId,
            datasetStorageAccountResource,
            billingProfile);

    // --- part 2 - create snapshot by full view of the ingested data
    Snapshot snapshot = new Snapshot().id(snapshotId);
    SnapshotTable snapshotTable = new SnapshotTable();
    snapshotTable.columns(destinationTable.getColumns());
    snapshotTable.id(destinationTable.getId());
    snapshotTable.name(destinationTable.getName());
    snapshot.snapshotTables(List.of(snapshotTable));

    // CreateSnapshotParquetFilesAzureStep
    // CreateSnapshotParquetFilesAzureStep part 1 - Create snapshot parquet files via external table
    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFiles(
            snapshot.getTables(),
            snapshotId,
            sourceDatasetDataSourceName,
            snapshotDataSourceName,
            isGlobalFileIds);
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "all_data_types"));
    // Test that parquet files are correctly generated
    String snapshotParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(destinationTable.getName());
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

    // CreateSnapshotParquetFilesAzureStep part 2
    // Create snapshot row ids parquet file via external table
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        snapshot.getTables(), snapshotId, snapshotDataSourceName, tableRowCounts);
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE));
    // Test that parquet files are correctly generated
    String snapshotRowIdsParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(PDAO_ROW_ID_TABLE);
    synapseUtils.addParquetFileName(snapshotRowIdsParquetFileName, snapshotStorageAccountResource);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(2));

    // CreateSnapshotCountTableRowsAzureStep
    snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    // Updated snapshot w/ rowId
    snapshotTable.rowCount(snapshotRowIds.size());
    snapshot.snapshotTables(List.of(snapshotTable));

    List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    assertThat("4 fileRefs Returned.", refIds.size(), equalTo(4));

    // Make sure all are valid UUIDs
    refIds.forEach(UUID::fromString);

    // do a basic query of the data
    snapshotQueryCredentialName =
        AzureSynapsePdao.getCredentialName(snapshot.getId(), TEST_USER.getEmail());
    snapshotQueryDataSourceName =
        AzureSynapsePdao.getDataSourceName(snapshot.getId(), TEST_USER.getEmail());
    azureSynapsePdao.getOrCreateExternalDataSource(
        snapshotSignUrlBlob, snapshotQueryCredentialName, snapshotQueryDataSourceName);
    List<Map<String, Optional<Object>>> tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getTableData(
                snapshotTable,
                snapshotTable.getName(),
                snapshotQueryDataSourceName,
                IngestUtils.getSnapshotParquetFilePathForQuery(snapshotTable.getName()),
                10,
                0,
                "first_name",
                SqlSortDirection.ASC,
                ""),
            isGlobalFileIds);
    assertThat(
        "table query contains correct data in the right order (ascending by first name)",
        tableData,
        contains(expectedData.get(0), expectedData.get(1)));

    // now swap the order
    tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getTableData(
                snapshotTable,
                snapshotTable.getName(),
                snapshotQueryDataSourceName,
                IngestUtils.getSnapshotParquetFilePathForQuery(snapshotTable.getName()),
                10,
                0,
                "first_name",
                SqlSortDirection.DESC,
                ""),
            isGlobalFileIds);
    assertThat(
        "table query contains correct data in the right order (descending by first name)",
        tableData,
        contains(expectedData.get(1), expectedData.get(0)));

    // now read a single value
    tableData =
        prepQueryResultForComparison(
            azureSynapsePdao.getTableData(
                snapshotTable,
                snapshotTable.getName(),
                snapshotQueryDataSourceName,
                IngestUtils.getSnapshotParquetFilePathForQuery(snapshotTable.getName()),
                10,
                0,
                "first_name",
                SqlSortDirection.ASC,
                "upper(first_name)='SALLY'"),
            isGlobalFileIds);
    assertThat(
        "table query contains only a single record", tableData, contains(expectedData.get(1)));

    //  clean out synapse
    // we'll do this in the test cleanup method, but it will be a step in the normal flight

  }

  private Optional<Object> extractFileId(Optional<Object> drsUri, boolean isGlobalFileIds) {
    return drsUri.map(
        d -> {
          DrsId drsId = DrsIdService.fromUri(d.toString());
          if (isGlobalFileIds) {
            assertThat("drs id is a v2 drs id", drsId.getVersion(), equalTo("v2"));
            assertNull("drs id has no snapshot", drsId.getSnapshotId());
            assertNotNull("drs id has no snapshot", drsId.getFsObjectId());
          } else {
            assertThat("drs id is a v1 drs id", drsId.getVersion(), equalTo("v1"));
            assertNotNull("drs id has no snapshot", drsId.getSnapshotId());
            assertNotNull("drs id has no snapshot", drsId.getFsObjectId());
          }
          return UUID.fromString(drsId.getFsObjectId());
        });
  }

  private List<Map<String, Optional<Object>>> prepQueryResultForComparison(
      List<Map<String, Optional<Object>>> tableData, Boolean isGlobalFileIds) {
    return tableData.stream()
        // Remove datarepo_row_id since it's random
        .peek(r -> r.remove(PDAO_ROW_ID_COLUMN))
        // Replace the DRS id with its file ID for easier comparison
        .peek(r -> r.put("file", extractFileId(r.get("file"), isGlobalFileIds)))
        .peek(r -> r.put("dirRefCol", extractFileId(r.get("dirRefCol"), isGlobalFileIds)))
        .toList();
  }
}
