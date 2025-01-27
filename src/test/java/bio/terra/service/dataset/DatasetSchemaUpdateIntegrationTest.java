package bio.terra.service.dataset;

import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration.User;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableModel;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

// TODO move me to integration dir
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class DatasetSchemaUpdateIntegrationTest {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private Users users;

  private User steward;
  private UUID profileId;
  private UUID datasetId;

  @BeforeEach
  public void setup() throws Exception {
    steward = users.steward();
    dataRepoFixtures.resetConfig(steward);
    profileId = dataRepoFixtures.createBillingProfile(steward).getId();
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward, profileId, "snapshot-test-dataset.json");
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "snapshot-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
  }

  @AfterEach
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward);
    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward, datasetId);
    }

    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward, profileId);
    }
  }

  @Test
  void testDatasetAddNewTableSuccess() throws Exception {
    String newTableName = "new_table";
    String newTableColumnName = "new_table_column";

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            DatasetFixtures.tableModel(
                                newTableName, List.of(newTableColumnName)))));
    DatasetModel response = dataRepoFixtures.updateSchema(steward, datasetId, updateModel);
    Optional<TableModel> newTable =
        response.getSchema().getTables().stream()
            .filter(tableModel -> tableModel.getName().equals(newTableName))
            .findFirst();
    assertThat("The new table is in the update response", newTable.isPresent());
    Optional<ColumnModel> newColumn =
        newTable.get().getColumns().stream()
            .filter(columnModel -> columnModel.getName().equals(newTableColumnName))
            .findFirst();
    assertThat(
        "The new table includes the new column in the update response", newColumn.isPresent());
  }

  @Test
  void testDatasetAddNewColumnSuccess() throws Exception {
    String existingTableName = "thetable";
    String existingTableColumnA = "added_column_a";
    String existingTableColumnB = "added_column_b";
    List<ColumnModel> newColumns =
        List.of(
            DatasetFixtures.columnModel(existingTableColumnA),
            DatasetFixtures.columnModel(existingTableColumnB));

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addColumns(
                        List.of(DatasetFixtures.columnUpdateModel(existingTableName, newColumns))));
    DatasetModel response = dataRepoFixtures.updateSchema(steward, datasetId, updateModel);
    Optional<TableModel> existingTable =
        response.getSchema().getTables().stream()
            .filter(tableModel -> tableModel.getName().equals(existingTableName))
            .findFirst();
    assertThat("The existing table is in the update response", existingTable.isPresent());
    boolean addedColumns =
        existingTable.get().getColumns().stream()
            .map(ColumnModel::getName)
            .toList()
            .containsAll(List.of(existingTableColumnA, existingTableColumnB));
    assertThat("The existing table includes the new columns in the update response", addedColumns);
  }

  @Test
  void testDatasetAddColumnToNewTableSuccess() throws Exception {
    String newTableName = "added_table";
    String newTableColumnName = "added_table_column";
    String anotherNewColumnName = "another_added_table_column";
    List<ColumnModel> newColumns = List.of(DatasetFixtures.columnModel(anotherNewColumnName));

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            DatasetFixtures.tableModel(newTableName, List.of(newTableColumnName))))
                    .addColumns(
                        List.of(DatasetFixtures.columnUpdateModel(newTableName, newColumns))));

    DatasetModel response = dataRepoFixtures.updateSchema(steward, datasetId, updateModel);
    Optional<TableModel> newTable =
        response.getSchema().getTables().stream()
            .filter(tableModel -> tableModel.getName().equals(newTableName))
            .findFirst();
    assertThat("The new table is in the update response", newTable.isPresent());
    boolean columns =
        newTable.get().getColumns().stream()
            .map(ColumnModel::getName)
            .toList()
            .containsAll(List.of(newTableColumnName, anotherNewColumnName));
    assertThat("The new table includes the new columns in the update response", columns);
  }

  @Test
  void testDatasetAddNewRelationshipSuccess() throws Exception {
    String relationshipName = "testRelationship";
    RelationshipModel relationshipModel =
        new RelationshipModel()
            .name(relationshipName)
            .from(new RelationshipTermModel().table("thetable").column("thecolumn"))
            .to(new RelationshipTermModel().table("anothertable").column("anothercolumn"));

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("Integration test relationship addition")
            .changes(
                new DatasetSchemaUpdateModelChanges().addRelationships(List.of(relationshipModel)));
    DatasetModel response = dataRepoFixtures.updateSchema(steward, datasetId, updateModel);
    Optional<RelationshipModel> createdRelationship =
        response.getSchema().getRelationships().stream()
            .filter(r -> r.getName().equals(relationshipName))
            .findFirst();
    assertThat("The new relationship is in the update response", createdRelationship.isPresent());
  }
}
