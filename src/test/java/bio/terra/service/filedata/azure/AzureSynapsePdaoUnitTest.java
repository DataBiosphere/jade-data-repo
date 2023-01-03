package bio.terra.service.filedata.azure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.service.common.AssetUtils;
import bio.terra.service.dataset.AssetSpecification;
import java.io.IOException;
import java.sql.SQLException;
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

  @Autowired AssetUtils assetUtils;
  private SnapshotRequestAssetModel requestAssetModel;
  private AssetSpecification assetSpec;
  @Autowired AzureSynapsePdao azureSynapsePdao;

  @MockBean
  @Qualifier("synapseJdbcTemplate")
  NamedParameterJdbcTemplate synapseJdbcTemplate;

  @Before
  public void setup() throws IOException {
    when(synapseJdbcTemplate.update(any(), any(MapSqlParameterSource.class))).thenReturn(2);
    assetSpec = assetUtils.buildTestAssetSpec();
    requestAssetModel =
        new SnapshotRequestAssetModel()
            .assetName(assetSpec.getName())
            .addRootValuesItem("sample2")
            .addRootValuesItem("sample3");
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
