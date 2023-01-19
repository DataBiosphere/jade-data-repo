package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.Relationship;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.grammar.Query;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.create.CreateSnapshotPrimaryDataQueryUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
public class AzureSynapsePdaoSnapshotConnectedTest {
  private static Logger logger = LoggerFactory.getLogger(AzureSynapsePdaoConnectedTest.class);

  private String randomFlightId;
  private String snapshotDataSourceName;
  private String sourceDatasetDataSourceName;
  private AzureStorageAccountResource datasetStorageAccountResource;
  private AzureStorageAccountResource snapshotStorageAccountResource;
  private BillingProfileModel billingProfile;
  private static final UUID snapshotId = UUID.randomUUID();

  @Autowired AzureSynapsePdao azureSynapsePdao;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired ConnectedOperations connectedOperations;
  @Autowired DatasetService datasetService;
  @MockBean private IamProviderInterface samService;
  @Autowired SynapseUtils synapseUtils;
  @Autowired SnapshotDao snapshotDao;
  @Autowired JsonLoader jsonLoader;
  @Autowired CreateSnapshotPrimaryDataQueryUtils createSnapshotPrimaryDataQueryUtils;

  @Before
  public void setup() throws Exception {
    synapseUtils.synapseTestSetup();
    connectedOperations.stubOutSamCalls(samService);

    // retrieve things setup in SynapseUtils
    datasetStorageAccountResource = synapseUtils.retrieveDatasetStorageAccountResource();
    billingProfile = synapseUtils.retrieveBillingProfileModel();
    snapshotStorageAccountResource = synapseUtils.retrieveSnapshotStorageAccountResource();
    randomFlightId = synapseUtils.retrieveRandomFlightId();
    sourceDatasetDataSourceName = synapseUtils.retrieveSourceDatasetDataSourceName();
    snapshotDataSourceName = synapseUtils.retrieveSnapshotDataSourceName();

    snapshot = new Snapshot().id(snapshotId);
    csvIngestRequestModel = new IngestRequestModel().format(FormatEnum.CSV).csvSkipLeadingRows(2);
    jsonIngestRequestModel = new IngestRequestModel().format(FormatEnum.JSON);
  }

  @After
  public void cleanup() throws Exception {
    synapseUtils.synapseTestCleanup();
    connectedOperations.teardown();
  }

  @Test(expected = PdaoException.class)
  public void testEmptySourceDatasetSnapshotByAsset() throws IOException, SQLException {
    DatasetTable allDataTypesTable =
        jsonLoader.loadObject("ingest-test-dataset-table-all-data-types.json", DatasetTable.class);

    // ----- SNAPSHOT ---
    // -- CreateSnapshotByAssetParquetFilesAzureStep --
    // Create snapshot parquet files via external table
    // By Asset
    String assetName = "testAsset";
    List<DatasetTable> datasetTables = List.of(allDataTypesTable);
    String rootTableName = allDataTypesTable.getName();
    String rootColumnName = "first_name";
    String rootValue = "Sally";
    azureSynapsePdao.createSnapshotParquetFilesByAsset(
        buildAssetSpecification(
            datasetTables, assetName, rootTableName, rootColumnName, new ArrayList<>()),
        snapshotId,
        sourceDatasetDataSourceName,
        snapshotDataSourceName,
        new SnapshotRequestAssetModel().assetName(assetName).addRootValuesItem(rootValue),
        false);
  }

  @Test
  public void testArraySnapshotByAsset() throws SQLException, IOException {
    // Prep dataset data for snapshot
    // Table 1 - Participant
    SnapshotTable participantSnapshotTable = setupParticipantTable();

    // Table 2 - Sample
    SnapshotTable sampleSnapshotTable = setupSampleTable();

    snapshot.snapshotTables(List.of(participantSnapshotTable, sampleSnapshotTable));

    // Define relationships
    // Test Array field
    AssetRelationship participantSampleRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(sampleTable.getColumnByName("participant_ids"))
                    .fromTable(sampleTable)
                    .toColumn(participantTable.getColumnByName("id"))
                    .toTable(participantTable)
                    .name("participant_sample_table"));
    List<AssetRelationship> relationships = List.of(participantSampleRelationship);

    // ----- SNAPSHOT ---

    // -- CreateSnapshotByAssetParquetFilesAzureStep --
    // Create snapshot parquet files via external table
    // By Asset
    String assetName = "testAsset";
    List<DatasetTable> datasetTables = List.of(participantTable, sampleTable);
    String rootTableName = sampleTable.getRawTableName();
    String rootColumnName = "participant_ids";
    String rootValue = "1";
    Map<String, Long> snapshotByAssetTableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByAsset(
            buildAssetSpecification(
                datasetTables, assetName, rootTableName, rootColumnName, relationships),
            snapshotId,
            sourceDatasetDataSourceName,
            snapshotDataSourceName,
            new SnapshotRequestAssetModel().assetName(assetName).addRootValuesItem(rootValue),
            false);
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "participant"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "sample"));

    String snapshotParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, participantTable.getName());
    synapseUtils.addParquetFileName(snapshotParquetFileName, snapshotStorageAccountResource);
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Sally", "Bobby", "Freddy")));
    assertThat(
        "Table row count should equal 3 for destination table",
        snapshotByAssetTableRowCounts.get(participantTable.getName()),
        equalTo(3L));
  }

  @Test
  public void testSnapshotByAsset() throws SQLException, IOException {
    setupFourTableDataset();

    // -- CreateSnapshotByAssetParquetFilesAzureStep --
    // Create snapshot parquet files via external table
    // By Asset
    String assetName = "testAsset";
    List<DatasetTable> datasetTables =
        List.of(allDataTypesTable, dateOfBirthTable, participantTable, sampleTable);
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
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "participant"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "date_of_birth"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "all_data_types"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "sample"));

    validateFourTableSnapshot(rootTableName, snapshotByAssetTableRowCounts);

    createSnapshotRowIdParquet(snapshotByAssetTableRowCounts);
  }

  @Test
  public void testSnapshotByQuery() throws SQLException, IOException {
    setupFourTableDataset();

    // -- CreateSnapshotByAssetParquetFilesAzureStep --
    // Create snapshot parquet files via external table
    // By Asset
    String datasetName = "testDataset";
    Map<String, DatasetModel> datasetMap =
        Collections.singletonMap(datasetName, new DatasetModel().name(datasetName));
    String assetName = "testAsset";
    List<DatasetTable> datasetTables =
        List.of(allDataTypesTable, dateOfBirthTable, participantTable, sampleTable);
    String rootTableName = participantTable.getRawTableName();
    String rootColumnName = "id";
    String userProvidedQuery =
        "SELECT testDataset.participant.datarepo_row_id FROM testDataset.participant WHERE testDataset.participant.id = '1'";
    Query query = Query.parse(userProvidedQuery);
    AssetSpecification assetSpecification =
        buildAssetSpecification(
            datasetTables, assetName, rootTableName, rootColumnName, relationships);
    createSnapshotPrimaryDataQueryUtils.validateRootTable(query, assetSpecification);

    SynapseVisitor synapseVisitor = new SynapseVisitor(datasetMap, sourceDatasetDataSourceName);
    String translatedQuery = query.translateSql(synapseVisitor);
    Map<String, Long> snapshotByQueryTableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByQuery(
            assetSpecification,
            snapshotId,
            sourceDatasetDataSourceName,
            snapshotDataSourceName,
            translatedQuery);
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "participant"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "date_of_birth"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "all_data_types"));
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, "sample"));

    validateFourTableSnapshot(rootTableName, snapshotByQueryTableRowCounts);

    createSnapshotRowIdParquet(snapshotByQueryTableRowCounts);
  }

  private void setupFourTableDataset() throws SQLException, IOException {
    // Prep dataset data for snapshot
    snapshot = new Snapshot().id(snapshotId);
    // Table 1 - All Data Types Table
    allDataTypesTable =
        synapseUtils.ingestIntoAllDataTypesTable(
            csvIngestRequestModel,
            "azure-simple-dataset-ingest-request.csv",
            randomFlightId,
            datasetStorageAccountResource,
            billingProfile);
    SnapshotTable allDataTypesSnapshotTable = setupSnapshotTable(allDataTypesTable);

    // Table 2 - date_of_birth
    String dateOfBirthTableIngestFlightId = UUID.randomUUID().toString();
    dateOfBirthTable =
        synapseUtils.ingestIntoTable(
            "ingest-test-dataset-table-date-of-birth.json",
            csvIngestRequestModel,
            "ingest-test-dataset-date-of-birth-rows.csv",
            dateOfBirthTableIngestFlightId,
            "synapsetestdata/test",
            4,
            datasetStorageAccountResource,
            billingProfile);
    SnapshotTable dateOfBirthSnapshotTable = setupSnapshotTable(dateOfBirthTable);

    // Table 3 - Participant
    SnapshotTable participantSnapshotTable = setupParticipantTable();

    // Table 4 - Sample
    SnapshotTable sampleSnapshotTable = setupSampleTable();

    snapshot.snapshotTables(
        List.of(
            allDataTypesSnapshotTable,
            dateOfBirthSnapshotTable,
            participantSnapshotTable,
            sampleSnapshotTable));

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
    // Test Array field
    AssetRelationship participantSampleRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(participantTable.getColumnByName("id"))
                    .fromTable(participantTable)
                    .toColumn(sampleTable.getColumnByName("participant_ids"))
                    .toTable(sampleTable)
                    .name("participant_sample_table"));
    relationships =
        List.of(
            participantDOBRelationship, participantADTRelationship, participantSampleRelationship);
  }

  private void validateFourTableSnapshot(String rootTableName, Map<String, Long> tableRowCounts) {
    // Check correct row included for root table
    String snapshotParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, rootTableName);
    synapseUtils.addParquetFileName(snapshotParquetFileName, snapshotStorageAccountResource);
    List<String> snapshotFirstNames =
        synapseUtils.readParquetFileStringColumn(
            snapshotParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of names in snapshot should equal the dataset names",
        snapshotFirstNames,
        equalTo(List.of("Sally")));
    assertThat(
        "Table row count should equal 1 for destination table",
        tableRowCounts.get(rootTableName),
        equalTo(1L));
    // Check correct row included for date of birth table, participant_id
    String dobParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, dateOfBirthTable.getName());
    synapseUtils.addParquetFileName(dobParquetFileName, snapshotStorageAccountResource);
    List<String> participantIds =
        synapseUtils.readParquetFileStringColumn(
            dobParquetFileName, snapshotDataSourceName, "participant_id", true);
    assertThat(
        "List of participant ids in snapshot should id for participant from root table",
        participantIds,
        equalTo(List.of("1")));
    assertThat(
        "Table row count should equal 1 for destination table",
        tableRowCounts.get(dateOfBirthTable.getName()),
        equalTo(1L));
    // Check correct row included for all_data_types, first_name Sally
    String adtParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, allDataTypesTable.getName());
    synapseUtils.addParquetFileName(adtParquetFileName, snapshotStorageAccountResource);
    List<String> firstNames =
        synapseUtils.readParquetFileStringColumn(
            adtParquetFileName, snapshotDataSourceName, "first_name", true);
    assertThat(
        "List of participant ids in snapshot should id for participant from root table",
        firstNames,
        equalTo(List.of("Sally")));
    assertThat(
        "Table row count should equal 1 for destination table",
        tableRowCounts.get(allDataTypesTable.getName()),
        equalTo(1L));
    // Check correct row included for sample, participant_id "1"
    String sampleParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, sampleTable.getName());
    synapseUtils.addParquetFileName(sampleParquetFileName, snapshotStorageAccountResource);
    List<String> ids =
        synapseUtils.readParquetFileStringColumn(
            sampleParquetFileName, snapshotDataSourceName, "id", true);
    assertThat(
        "List of participant ids in snapshot should id for participant from root table",
        ids,
        equalTo(List.of("abc", "part1")));
    assertThat(
        "Table row count should equal 2 for destination table",
        tableRowCounts.get(sampleTable.getName()),
        equalTo(2L));
  }

  private void createSnapshotRowIdParquet(Map<String, Long> tableRowCounts) throws SQLException {
    // 7 - Create snapshot row ids parquet file via external table
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        snapshot.getTables(), snapshotId, snapshotDataSourceName, tableRowCounts);
    synapseUtils.addTableName(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE));
    String snapshotRowIdsParquetFileName =
        IngestUtils.getSnapshotParquetFilePathForQuery(snapshotId, PDAO_ROW_ID_TABLE);
    synapseUtils.addParquetFileName(snapshotRowIdsParquetFileName, snapshotStorageAccountResource);
    List<String> snapshotRowIds =
        synapseUtils.readParquetFileStringColumn(
            snapshotRowIdsParquetFileName, snapshotDataSourceName, PDAO_ROW_ID_COLUMN, true);
    assertThat("Snapshot contains expected number or rows", snapshotRowIds.size(), equalTo(5));
  }

  private SnapshotTable setupParticipantTable() throws SQLException, IOException {
    String participantTableIngestFlightId = UUID.randomUUID().toString();
    participantTable =
        synapseUtils.ingestIntoTable(
            "ingest-test-dataset-table-participant.json",
            jsonIngestRequestModel,
            "ingest-test-dataset-participant-rows.json",
            participantTableIngestFlightId,
            "synapsetestdata/test",
            4,
            datasetStorageAccountResource,
            billingProfile);
    return setupSnapshotTable(participantTable);
  }

  private SnapshotTable setupSampleTable() throws SQLException, IOException {
    String sampleTableIngestFlightId = UUID.randomUUID().toString();
    sampleTable =
        synapseUtils.ingestIntoTable(
            "ingest-test-dataset-table-sample.json",
            jsonIngestRequestModel,
            "ingest-test-dataset-sample-rows.json",
            sampleTableIngestFlightId,
            "synapsetestdata/test",
            3,
            datasetStorageAccountResource,
            billingProfile);
    return setupSnapshotTable(sampleTable);
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
}
