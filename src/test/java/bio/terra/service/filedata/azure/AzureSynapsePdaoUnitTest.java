package bio.terra.service.filedata.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.exception.TableNotFoundException;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.tabulardata.WalkRelationship;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.stringtemplate.v4.ST;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class AzureSynapsePdaoUnitTest {
  private SnapshotRequestAssetModel requestAssetModel;
  private AssetSpecification assetSpec;
  private WalkRelationship walkRelationship;

  @Autowired AssetUtils assetUtils;
  @SpyBean AzureSynapsePdao azureSynapsePdaoSpy;

  @MockBean
  @Qualifier("synapseJdbcTemplate")
  NamedParameterJdbcTemplate synapseJdbcTemplate;

  @Before
  public void setup() throws IOException, SQLException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class))).thenReturn(2);
    assetSpec = assetUtils.buildTestAssetSpec();
    walkRelationship = assetUtils.buildExampleWalkRelationship(assetSpec);
  }

  @Test
  public void createSnapshotParquetFilesByAsset() throws SQLException {
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFilesByAsset(
            assetSpec,
            UUID.randomUUID(),
            "datasetDataSource1",
            "snapshotDataSource1",
            requestAssetModel,
            false);
    assertThat(
        "Table has 2 rows",
        tableRowCounts.get(assetSpec.getRootTable().getTable().getName()),
        equalTo(2L));
  }

  @Test(expected = PdaoException.class)
  public void createSnapshotParquetFilesByAssetNoRows() throws SQLException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class)))
        .thenThrow(new DataAccessException("...") {});
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    azureSynapsePdaoSpy.createSnapshotParquetFilesByAsset(
        assetSpec,
        UUID.randomUUID(),
        "datasetDataSource1",
        "snapshotDataSource1",
        requestAssetModel,
        false);
  }

  @Test(expected = PdaoException.class)
  public void createSnapshotParquetFilesByAssetZeroRows() throws SQLException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class))).thenReturn(0);
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    azureSynapsePdaoSpy.createSnapshotParquetFilesByAsset(
        assetSpec,
        UUID.randomUUID(),
        "datasetDataSource1",
        "snapshotDataSource1",
        requestAssetModel,
        false);
  }

  @Test
  public void createSnapshotParquetFilesByRelationship() throws SQLException {
    Map<String, Long> tableRowCounts = new HashMap<>();
    // Pre-populate 'FROM' table's row count
    tableRowCounts.put("participant", Long.valueOf(2));
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());

    azureSynapsePdaoSpy.createSnapshotParquetFilesByRelationship(
        UUID.randomUUID(),
        assetSpec,
        walkRelationship,
        "datasetDataSourceName1",
        "snapshotDataSourceName1",
        tableRowCounts,
        false);
    assertThat(
        "Correct row count returned for sample table", tableRowCounts.get("sample"), equalTo(3L));
  }

  @Test
  public void createSnapshotParquetFilesByRelationshipNoFromTableRows() throws SQLException {
    Map<String, Long> tableRowCounts = new HashMap<>();
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());

    azureSynapsePdaoSpy.createSnapshotParquetFilesByRelationship(
        UUID.randomUUID(),
        assetSpec,
        walkRelationship,
        "datasetDataSourceName1",
        "snapshotDataSourceName1",
        tableRowCounts,
        false);
    assertThat(
        "'FROM' table was not in tableRowCounts, so no rows were added",
        tableRowCounts.get("sample"),
        equalTo(0L));
  }

  @Test
  public void buildSnapshotByAssetQueryTemplateNoEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    ST query = azureSynapsePdaoSpy.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "No existing entries in TableRowCounts, so the query should NOT include additional clause to avoid rows already added to snapshot",
        query.render(),
        not(containsString("already_existing_to_rows")));
  }

  @Test
  public void buildSnapshotByAssetQueryTemplateZeroEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put("table1", Long.valueOf(0));
    ST query = azureSynapsePdaoSpy.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "Zero rows included in TableRowCounts, so the query should NOT include additional clause to avoid rows already added to snapshot",
        query.render(),
        not(containsString("already_existing_to_rows")));
  }

  @Test
  public void buildSnapshotByAssetQueryTemplatePositiveEntry() {
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put("table1", Long.valueOf(2));
    ST query = azureSynapsePdaoSpy.buildSnapshotByAssetQueryTemplate(tableRowCounts, "table1");
    assertThat(
        "There were already rows in the table, so the query should include additional clause to avoid rows already added to snapshot",
        query.render(),
        containsString("already_existing_to_rows"));
  }

  @Test(expected = InvalidSnapshotException.class)
  public void testCreateSnapshotRowIdsParquetFileNoRows() throws SQLException {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1");
    List<SnapshotTable> tables = List.of(snapshotTable1);
    Map<String, Long> tableRowCounts = new HashMap<>();
    // empty tableRowCounts, so should throw InvalidSnapshotException
    azureSynapsePdaoSpy.createSnapshotRowIdsParquetFile(
        tables, UUID.randomUUID(), "snapshotDataSourceName1", tableRowCounts);
  }

  @Test
  public void testCreateSnapshotRowIdsParquetFile() throws SQLException {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    Map<String, Long> tableRowCounts = new HashMap<>();
    tableRowCounts.put(snapshotTable1.getName(), 3L);
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());
    azureSynapsePdaoSpy.createSnapshotRowIdsParquetFile(
        tables, UUID.randomUUID(), "snapshotDataSourceName1", tableRowCounts);
  }

  @Test(expected = TableNotFoundException.class)
  public void testCreateSnapshotParquetFilesByRowIdNoRequestTable() throws SQLException {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    azureSynapsePdaoSpy.createSnapshotParquetFilesByRowId(
        tables,
        UUID.randomUUID(),
        "datasetDataSourceName1",
        "snapshotDataSourceName1",
        new SnapshotRequestRowIdModel(),
        false);
  }

  @Test
  public void testCreateSnapshotParquetFilesByRowId() throws SQLException {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    SnapshotRequestRowIdTableModel tableModel =
        new SnapshotRequestRowIdTableModel().tableName("table1");
    SnapshotRequestRowIdModel requestRowIdModel =
        new SnapshotRequestRowIdModel().tables(List.of(tableModel));

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFilesByRowId(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            requestRowIdModel,
            false);
    assertThat("Table has 2 rows", tableRowCounts.get("table1"), equalTo(2L));
  }

  @Test
  public void testCreateSnapshotParquetFilesByRowIdNoRows() throws SQLException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class)))
        .thenThrow(new DataAccessException("...") {});
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    SnapshotRequestRowIdTableModel tableModel =
        new SnapshotRequestRowIdTableModel().tableName("table1");
    SnapshotRequestRowIdModel requestRowIdModel =
        new SnapshotRequestRowIdModel().tables(List.of(tableModel));

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFilesByRowId(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            requestRowIdModel,
            false);
    assertThat(
        "Table should have thrown exception, should should be set to 0 rows",
        tableRowCounts.get("table1"),
        equalTo(0L));
  }

  @Test
  public void testCreateSnapshotParquetFiles() throws SQLException {
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);
    doReturn(3).when(azureSynapsePdaoSpy).executeSynapseQuery(any());

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFiles(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            "datasetFlightId1",
            false);
    assertThat("Table has 3 rows", tableRowCounts.get("table1"), equalTo(3L));
  }

  @Test
  public void testCreateSnapshotParquetFilesNoRows() throws SQLException {
    doThrow(SQLServerException.class).when(azureSynapsePdaoSpy).executeSynapseQuery(any());
    SnapshotTable snapshotTable1 = new SnapshotTable().name("table1").id(UUID.randomUUID());
    List<SnapshotTable> tables = List.of(snapshotTable1);

    Map<String, Long> tableRowCounts =
        azureSynapsePdaoSpy.createSnapshotParquetFiles(
            tables,
            UUID.randomUUID(),
            "datasetDataSourceName1",
            "snapshotDataSourceName1",
            "datasetFlightId1",
            false);
    assertThat("Table has 0 rows", tableRowCounts.get("table1"), equalTo(0L));
  }
}
