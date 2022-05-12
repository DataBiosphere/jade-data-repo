package bio.terra.service.dataset.flight.update;

import static bio.terra.common.TestUtils.bigQueryProjectForDatasetName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.Column;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.Optional;
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
public class DatasetSchemaUpdateConnectedTest {

  @Autowired private JsonLoader jsonLoader;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private DatasetDao datasetDao;
  @Autowired private DatasetTableDao datasetTableDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BigQueryDatasetPdao bigQueryDatasetPdao;
  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private DatasetSummaryModel summaryModel;
  private static final Logger logger =
      LoggerFactory.getLogger(DatasetSchemaUpdateConnectedTest.class);

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    // create a dataset and check that it succeeds
    String resourcePath = "snapshot-test-dataset.json";
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
    datasetRequest
        .name(Names.randomizeName(datasetRequest.getName()))
        .defaultProfileId(billingProfile.getId());
    summaryModel = connectedOperations.createDataset(datasetRequest);
    logger.info("--------begin test---------");
  }

  @After
  public void tearDown() throws Exception {
    logger.info("--------start of tear down---------");

    configService.reset();
    connectedOperations.teardown();
  }

  @Test
  public void testTableAdditionSteps() throws Exception {
    String tableName = "added_table";
    String columnName = "added_table_column";
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("Connected test table addition")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            new TableModel()
                                .name(tableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(columnName)
                                            .datatype(TableDataType.STRING)
                                            .required(false)
                                            .arrayOf(false))))));

    UUID datasetId = summaryModel.getId();
    FlightContext flightContext = mock(FlightContext.class);
    DatasetSchemaUpdateAddTablesPostgresStep postgresStep =
        new DatasetSchemaUpdateAddTablesPostgresStep(datasetTableDao, datasetId, updateModel);

    postgresStep.doStep(flightContext);

    Dataset postTableUpdateDataset = datasetDao.retrieve(datasetId);
    Optional<DatasetTable> newTable = postTableUpdateDataset.getTableByName(tableName);

    assertThat("updating a table's schema adds a new table", newTable.isPresent());

    Optional<Column> newColumn =
        newTable.flatMap(
            t -> t.getColumns().stream().filter(c -> c.getName().equals(columnName)).findFirst());

    assertThat("adding a table adds columns as well", newColumn.isPresent());

    String datasetName = summaryModel.getName();
    String bqDatasetName = BigQueryPdao.prefixName(datasetName);
    BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetDao, datasetName);
    String bigQueryProjectId = bigQueryProject.getProjectId();
    BigQuery bigQuery = bigQueryProject.getBigQuery();

    DatasetSchemaUpdateAddTablesBigQueryStep bigQueryStep =
        new DatasetSchemaUpdateAddTablesBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);

    bigQueryStep.doStep(flightContext);

    Table bigQueryTable =
        bigQuery.getTable(TableId.of(bigQueryProjectId, bqDatasetName, tableName));
    List<String> columnNames =
        bigQueryTable.getDefinition().getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());

    assertThat(
        "BigQuery has the new tables and columns",
        columnNames,
        hasItems("datarepo_row_id", columnName));

    bigQueryStep.undoStep(flightContext);
    postgresStep.undoStep(flightContext);

    Dataset postUndoDataset = datasetDao.retrieve(datasetId);
    newTable = postUndoDataset.getTableByName(tableName);

    assertThat("undoing the step removes the dataset table from postgres", newTable.isEmpty());
    bigQueryTable = bigQuery.getTable(TableId.of(bigQueryProjectId, bqDatasetName, tableName));

    assertThat(
        "undoing the step removes dataset table and columns from BigQuery", bigQueryTable == null);
  }
}
