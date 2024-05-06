package bio.terra.service.filedata.azure;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.tabulardata.WalkRelationship;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.stringtemplate.v4.ST;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureSynapsePdaoUnitTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private SnapshotRequestAssetModel requestAssetModel;
  private AssetSpecification assetSpec;
  private WalkRelationship walkRelationship;

  private AzureSynapsePdao azureSynapsePdao;

  @Mock NamedParameterJdbcTemplate synapseJdbcTemplate;
  @Mock AzureResourceConfiguration azureResourceConfiguration;

  @BeforeEach
  void setup() {
    azureSynapsePdao =
        new AzureSynapsePdao(
            azureResourceConfiguration,
            mock(ApplicationConfiguration.class),
            mock(DrsIdService.class),
            new ObjectMapper(),
            synapseJdbcTemplate);
    assetSpec = AssetUtils.buildTestAssetSpec();
    walkRelationship = AssetUtils.buildExampleWalkRelationship(assetSpec);
  }

  private void mockSynapse() {
    when(azureResourceConfiguration.synapse())
        .thenReturn(mock(AzureResourceConfiguration.Synapse.class));
  }

  private void mockUpdate(int value) {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class))).thenReturn(value);
  }

  @Test
  void createSnapshotParquetFilesByAsset() throws SQLException {
    mockSynapse();
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");
    mockUpdate(2);

    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByAsset(
            assetSpec,
            UUID.randomUUID(),
            "datasetDataSource1",
            "snapshotDataSource1",
            requestAssetModel,
            false,
            null);
    assertThat(
        "Table has 2 rows",
        tableRowCounts.get(assetSpec.getRootTable().getTable().getName()),
        equalTo(2L));
  }

  @Test
  void createSnapshotParquetFilesByAssetNoRows() {
    mockSynapse();
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class)))
        .thenThrow(new DataAccessException("...") {});
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    UUID snapshotId = UUID.randomUUID();
    assertThrows(
        PdaoException.class,
        () ->
            azureSynapsePdao.createSnapshotParquetFilesByAsset(
                assetSpec,
                snapshotId,
                "datasetDataSource1",
                "snapshotDataSource1",
                requestAssetModel,
                false,
                null));
  }

  @Test
  void createSnapshotParquetFilesByAssetZeroRows() {
    mockSynapse();
    mockUpdate(0);
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    UUID snapshotId = UUID.randomUUID();
    assertThrows(
        PdaoException.class,
        () ->
            azureSynapsePdao.createSnapshotParquetFilesByAsset(
                assetSpec,
                snapshotId,
                "datasetDataSource1",
                "snapshotDataSource1",
                requestAssetModel,
                false,
                null));
  }

  @Test
  void createSnapshotParquetFilesByRelationship() throws SQLException {
    mockSynapse();

    Map<String, Long> tableRowCounts = new HashMap<>();
    // Pre-populate 'FROM' table's row count
    tableRowCounts.put("participant", 2L);
    AzureSynapsePdao azureSynapsePdaoSpy = spy(azureSynapsePdao);
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());

    azureSynapsePdaoSpy.createSnapshotParquetFilesByRelationship(
        UUID.randomUUID(),
        assetSpec,
        walkRelationship,
        "datasetDataSourceName1",
        "snapshotDataSourceName1",
        tableRowCounts,
        false,
        null);
    assertThat(
        "Correct row count returned for sample table", tableRowCounts.get("sample"), equalTo(3L));
  }

  @Test
  void createSnapshotParquetFilesByRelationshipNoFromTableRows() {
    Map<String, Long> tableRowCounts = new HashMap<>();

    azureSynapsePdao.createSnapshotParquetFilesByRelationship(
        UUID.randomUUID(),
        assetSpec,
        walkRelationship,
        "datasetDataSourceName1",
        "snapshotDataSourceName1",
        tableRowCounts,
        false,
        null);
    assertThat(
        "'FROM' table was not in tableRowCounts, so no rows were added",
        tableRowCounts.get("sample"),
        equalTo(0L));
  }

  @Test
  void buildSnapshotByAssetQueryTemplateNoEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    ST query = azureSynapsePdao.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "No existing entries in TableRowCounts, so the query should NOT include additional clause to avoid rows already added to snapshot",
        query.render(),
        not(containsString("already_existing_to_rows")));
  }

  @Test
  void buildSnapshotByAssetQueryTemplateZeroEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put("table1", 0L);
    ST query = azureSynapsePdao.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "Zero rows included in TableRowCounts, so the query should NOT include additional clause to avoid rows already added to snapshot",
        query.render(),
        not(containsString("already_existing_to_rows")));
  }

  @Test
  void buildSnapshotByAssetQueryTemplatePositiveEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put("table1", 2L);
    ST query = azureSynapsePdao.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "There were already rows in the table, so the query should include additional clause to avoid rows already added to snapshot",
        query.render(),
        containsString("already_existing_to_rows"));
  }

  @Test
  void testCreateSnapshotRowIdsParquetFileNoRows() {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1");
    List<SnapshotTable> tables = List.of(snapshotTable1);
    Map<String, Long> tableRowCounts = new HashMap<>();
    // empty tableRowCounts, so should throw InvalidSnapshotException
    UUID snapshotId = UUID.randomUUID();
    assertThrows(
        InvalidSnapshotException.class,
        () ->
            azureSynapsePdao.createSnapshotRowIdsParquetFile(
                tables, snapshotId, "snapshotDataSourceName1", tableRowCounts));
  }

  @Test
  void testCreateSnapshotRowIdsParquetFile() throws SQLException {
    mockSynapse();
    UUID tableId = UUID.randomUUID();
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(tableId);
    List<SnapshotTable> tables = List.of(snapshotTable1);
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put(snapshotTable1.getName(), 3L);
    AzureSynapsePdao azureSynapsePdaoSpy = spy(azureSynapsePdao);
    ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(queryCaptor.capture());
    UUID snapshotId = UUID.randomUUID();
    azureSynapsePdaoSpy.createSnapshotRowIdsParquetFile(
        tables, snapshotId, "snapshotDataSourceName1", tableRowCounts);
    var expected =
        """
        CREATE EXTERNAL TABLE [%s]
            WITH (
                LOCATION = 'metadata/parquet/datarepo_row_ids/datarepo_row_ids.parquet',
                DATA_SOURCE = [snapshotDataSourceName1],
                FILE_FORMAT = []) AS
        SELECT '%s' as datarepo_table_id, datarepo_row_id
        FROM OPENROWSET(
                     BULK 'metadata/parquet/table1/*.parquet/*',
                     DATA_SOURCE = 'snapshotDataSourceName1',
                     FORMAT = 'parquet') AS rows;"""
            .formatted(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE), tableId);
    assertThat(queryCaptor.getValue(), equalToCompressingWhiteSpace(expected));
  }

  @Test
  void testCreateSnapshotParquetFilesByRowIdNoRequestTable() {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    UUID snapshotId = UUID.randomUUID();
    SnapshotRequestRowIdModel rowIdModel = new SnapshotRequestRowIdModel();
    assertThrows(
        TableNotFoundException.class,
        () ->
            azureSynapsePdao.createSnapshotParquetFilesByRowId(
                tables,
                snapshotId,
                "datasetDataSourceName1",
                "snapshotDataSourceName1",
                rowIdModel,
                false,
                null));
  }

  @Test
  void testCreateSnapshotParquetFilesByRowId() throws SQLException {
    mockSynapse();
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    SnapshotRequestRowIdTableModel tableModel =
        new SnapshotRequestRowIdTableModel().tableName("table1");
    SnapshotRequestRowIdModel requestRowIdModel =
        new SnapshotRequestRowIdModel().tables(List.of(tableModel));

    mockUpdate(2);

    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByRowId(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            requestRowIdModel,
            false,
            null);
    assertThat("Table has 2 rows", tableRowCounts.get("table1"), equalTo(2L));
  }

  @Test
  void testCreateSnapshotParquetFilesByRowIdNoRows() throws SQLException {
    mockSynapse();
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class)))
        .thenThrow(new DataAccessException("...") {});
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    SnapshotRequestRowIdTableModel tableModel =
        new SnapshotRequestRowIdTableModel().tableName("table1");
    SnapshotRequestRowIdModel requestRowIdModel =
        new SnapshotRequestRowIdModel().tables(List.of(tableModel));

    Map<String, Long> tableRowCounts =
        azureSynapsePdao.createSnapshotParquetFilesByRowId(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            requestRowIdModel,
            false,
            null);
    assertThat(
        "Table should have thrown exception, should should be set to 0 rows",
        tableRowCounts.get("table1"),
        equalTo(0L));
  }

  @Test
  void testCreateSnapshotParquetFiles() throws SQLException {
    mockSynapse();
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    AzureSynapsePdao azureSynapsePdaoSpy = spy(azureSynapsePdao);
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFiles(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            false,
            null);
    assertThat("Table has 3 rows", tableRowCounts.get("table1"), equalTo(3L));
  }

  @Test
  void testCreateSnapshotParquetFilesNoRows() throws SQLException {
    mockSynapse();
    AzureSynapsePdao azureSynapsePdaoSpy = spy(azureSynapsePdao);
    doThrow(SQLServerException.class).when(azureSynapsePdaoSpy).executeSynapseQuery(any());
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFiles(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            false,
            null);
    assertThat("Table has 0 rows", tableRowCounts.get("table1"), equalTo(0L));
  }

  @Test
  void getOrCreateExternalAzureDataSource() throws Exception {
    UUID id = UUID.randomUUID();
    AccessInfoModel accessInfoModel =
        new AccessInfoModel()
            .parquet(
                new AccessInfoParquetModel()
                    .sasToken(
                        "sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&sr=b&sig=mysig")
                    .url("https://fake.url"));
    AzureSynapsePdao azureSynapsePdaoSpy = spy(azureSynapsePdao);
    doNothing().when(azureSynapsePdaoSpy).getOrCreateExternalDataSource(any(), any(), any());
    assertThat(
        azureSynapsePdaoSpy.getOrCreateExternalDataSourceForResource(
            accessInfoModel, id, TEST_USER),
        equalTo(String.format("ds-%s-%s", id, TEST_USER.getEmail())));
  }
}
