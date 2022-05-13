package bio.terra.service.dataset;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.DataRepoResponse;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.JobModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

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

  private String stewardToken;
  private UUID profileId;

  private UUID datasetId;

  @Before
  public void setup() throws Exception {
    super.setup();
    stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    dataRepoFixtures.resetConfig(steward());
    profileId = dataRepoFixtures.createBillingProfile(steward()).getId();
    DatasetSummaryModel datasetSummaryModel =
        dataRepoFixtures.createDataset(steward(), profileId, "snapshot-test-dataset.json");
    datasetId = datasetSummaryModel.getId();
  }

  @After
  public void teardown() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    if (datasetId != null) {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }
    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
  }

  @Test
  public void testDatasetSchemaUpdatesSuccess() throws Exception {
    String newTableName = "added_table";
    String existingTableName = "thetable";

    String newTableColumnName = "added_table_column";
    String anotherNewColumnName = "another_added_table_column";
    String existingTableColumnA = "added_column_a";
    String existingTableColumnB = "added_column_b";

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
                                            .datatype(TableDataType.STRING)))))
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
                                            .datatype(TableDataType.STRING))),
                            new DatasetSchemaColumnUpdateModel()
                                .tableName(newTableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(anotherNewColumnName)
                                            .datatype(TableDataType.STRING))))));
    DataRepoResponse<JobModel> jobModelDataRepoResponse =
        dataRepoFixtures.updateSchema(steward(), datasetId, updateModel);
    System.out.println(jobModelDataRepoResponse);
  }
}
