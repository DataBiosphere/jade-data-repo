package bio.terra.service.filedata.azure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.DatasetTable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class AzureSynapsePdaoUnitTest {

  @Autowired JsonLoader jsonLoader;
  private SnapshotRequestAssetModel requestAssetModel;
  private AssetSpecification assetSpec;
  @Autowired AzureSynapsePdao azureSynapsePdao;

  @MockBean
  @Qualifier("synapseJdbcTemplate")
  NamedParameterJdbcTemplate synapseJdbcTemplate;

  @Before
  public void setup() throws IOException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class))).thenReturn(2);
    AssetTable assetTable_sample = setUpAssetTable("ingest-test-dataset-table-sample.json");
    AssetTable assetTable_participant =
        setUpAssetTable("ingest-test-dataset-table-participant.json");
    AssetTable assetTable_file = setUpAssetTable("ingest-test-dataset-table-file.json");

    String assetName = "sample_centric";
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetName)
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");

    assetSpec =
        new AssetSpecification()
            .name(assetName)
            .assetTables(List.of(assetTable_file, assetTable_sample, assetTable_participant))
            .rootTable(assetTable_sample)
            .rootColumn(
                assetTable_sample.getColumns().stream()
                    .filter(c -> c.getDatasetColumn().getName().equals("id"))
                    .findFirst()
                    .orElseThrow());
  }

  private AssetTable setUpAssetTable(String resourcePath) throws IOException {
    DatasetTable datasetTable = jsonLoader.loadObject(resourcePath, DatasetTable.class);
    datasetTable.id(UUID.randomUUID());
    List<AssetColumn> columns = new ArrayList<>();
    datasetTable.getColumns().stream()
        .forEach(c -> columns.add(new AssetColumn().datasetColumn(c).datasetTable(datasetTable)));
    return new AssetTable().datasetTable(datasetTable).columns(columns);
  }

  @Test
  public void createSnapshotParquetFilesByAsset() throws SQLException {

    azureSynapsePdao.createSnapshotParquetFilesByAsset(
        assetSpec,
        UUID.randomUUID(),
        "datasetDataSource1",
        "snapshotDataSource1",
        "flight1",
        requestAssetModel);
  }
}
