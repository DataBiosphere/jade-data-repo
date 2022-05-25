package bio.terra.service.dataset;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSpecificationModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import bio.terra.service.snapshot.SnapshotIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;

// TODO move me to integration dir
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetSchemaUpdateIntegrationTest extends UsersBase {

  @Autowired private AuthService authService;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private TestConfiguration testConfiguration;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private static final Logger logger = LoggerFactory.getLogger(DatasetSchemaUpdateIntegrationTest.class);
  private String stewardToken;
  private UUID profileId;
  private List<UUID> createdDatasetIds = new ArrayList<>();

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());
    createdDatasetIds.forEach(
        datasetId -> {
          try {
            dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
          } catch (Exception ex) {
            logger.warn("cleanup failed when deleting snapshot " + datasetId);
            ex.printStackTrace();
          }
        });

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testDatasetAddNewTableSuccess() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "snapshot-test-dataset.json");
    UUID datasetId = datasetSummaryModel.getId();
    createdDatasetIds.add(datasetId);

    String newTableName = "new_table";
    String newTableColumnName = "new_table_column";

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            new TableModel()
                                .name(newTableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(newTableColumnName)
                                            .datatype(TableDataType.STRING))))));
    DatasetSpecificationModel response = dataRepoFixtures.updateSchema(steward(), datasetId, updateModel);
    Optional<TableModel> newTable = response.getTables().stream()
        .filter(tableModel -> tableModel.getName().equals(newTableName))
        .findFirst();
    assertThat("The new table is in the update response", newTable.isPresent());
    Optional <ColumnModel> newColumn = newTable.get().getColumns().stream()
        .filter(columnModel -> columnModel.getName().equals(newTableColumnName)).findFirst();
    assertThat("The new table includes the new column in the update response",
        newColumn.isPresent());
  }

  @Test
  public void testDatasetAddNewColumnSuccess() throws Exception {
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "snapshot-test-dataset.json");
    UUID datasetId = datasetSummaryModel.getId();
    createdDatasetIds.add(datasetId);

    String existingTableName = "thetable";
    String existingTableColumnA = "added_column_a";
    String existingTableColumnB = "added_column_b";
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addColumns(
                        List.of(
                            new DatasetSchemaColumnUpdateModel()
                                .tableName(existingTableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(existingTableColumnA)
                                            .datatype(TableDataType.STRING),
                                        new ColumnModel()
                                            .name(existingTableColumnB)
                                            .datatype(TableDataType.STRING))))));
    DatasetSpecificationModel response = dataRepoFixtures.updateSchema(steward(), datasetId, updateModel);
    Optional<TableModel> existingTable = response.getTables().stream()
        .filter(tableModel -> tableModel.getName().equals(existingTableName))
        .findFirst();
    assertThat("The new table is in the update response", existingTable.isPresent());
    boolean newColumns = existingTable.get().getColumns().stream()
        .map(ColumnModel::getName)
        .collect(Collectors.toList())
        .containsAll(List.of(existingTableColumnA, existingTableColumnB));
    assertThat("The new table includes the new column in the update response", newColumns);
  }


}
