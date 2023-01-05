package bio.terra.service.filedata.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.tabulardata.WalkRelationship;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
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

    azureSynapsePdaoSpy.createSnapshotParquetFilesByAsset(
        assetSpec,
        UUID.randomUUID(),
        "datasetDataSource1",
        "snapshotDataSource1",
        requestAssetModel);
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
        tableRowCounts);
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
        tableRowCounts);
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
}
