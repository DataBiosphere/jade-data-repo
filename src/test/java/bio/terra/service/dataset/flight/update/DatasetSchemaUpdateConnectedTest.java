package bio.terra.service.dataset.flight.update;

import static bio.terra.common.TestUtils.bigQueryProjectForDatasetName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.Column;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.Relationship;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.Names;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSchemaColumnUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetRelationshipDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetTableDao;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
  @Autowired private DatasetService datasetService;
  @Autowired private DatasetTableDao datasetTableDao;
  @Autowired private ConfigurationService configService;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Autowired private DatasetRelationshipDao relationshipDao;
  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private DatasetSummaryModel summaryModel;
  private UUID datasetId;
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
    datasetId = summaryModel.getId();
    relationshipDao = spy(relationshipDao);
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
    List<String> bigQueryTableNames = newTable.get().getBigQueryTableNames();

    DatasetSchemaUpdateAddTablesBigQueryStep bigQueryStep =
        new DatasetSchemaUpdateAddTablesBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);

    bigQueryStep.doStep(flightContext);

    for (String bigQueryTable : bigQueryTableNames) {
      assertTrue(
          String.format("BigQuery has the new table %s", bigQueryTable),
          bigQueryDatasetPdao.tableExists(postTableUpdateDataset, bigQueryTable));
    }

    Set<String> columnNames =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, newTable.get().getName()).keySet();
    assertThat(
        "BigQuery view has expected columns", columnNames, hasItems("datarepo_row_id", columnName));

    bigQueryStep.undoStep(flightContext);
    postgresStep.undoStep(flightContext);

    Dataset postUndoDataset = datasetDao.retrieve(datasetId);
    newTable = postUndoDataset.getTableByName(tableName);

    assertThat("undoing the step removes the dataset table from postgres", newTable.isEmpty());

    for (String postUndoBigQueryTable : bigQueryTableNames) {
      assertFalse(
          String.format(
              "undoing the step removes dataset table %s from BigQuery", postUndoBigQueryTable),
          bigQueryDatasetPdao.tableExists(postUndoDataset, postUndoBigQueryTable));
    }
  }

  @Test
  public void testColumnAdditionSteps() throws Exception {
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

    DatasetSchemaUpdateAddColumnsPostgresStep postgresColumnStep =
        new DatasetSchemaUpdateAddColumnsPostgresStep(datasetTableDao, datasetId, updateModel);
    DatasetSchemaUpdateAddColumnsBigQueryStep bigQueryColumnStep =
        new DatasetSchemaUpdateAddColumnsBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);

    FlightContext flightContext = mock(FlightContext.class);
    postgresColumnStep.doStep(flightContext);
    Dataset postUpdateDataset = datasetDao.retrieve(datasetId);
    DatasetTable existingTable = postUpdateDataset.getTableByName(existingTableName).orElseThrow();
    Map<String, Column> existingTableColumnsMap = existingTable.getColumnsMap();
    assertThat(
        "the first table column was added to the existing table",
        existingTableColumnsMap.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was added to the existing table",
        existingTableColumnsMap.containsKey(existingTableColumnB));

    bigQueryColumnStep.doStep(flightContext);
    String datasetName = summaryModel.getName();
    String bqDatasetName = BigQueryPdao.prefixName(datasetName);
    BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetDao, datasetName);

    Map<String, Field> existingTableColumnNames =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, existingTable.getName());
    assertThat(
        "the first table column was added to the existing BigQuery table",
        existingTableColumnNames.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was added to the existing BigQuery table",
        existingTableColumnNames.containsKey(existingTableColumnB));

    bigQueryColumnStep.undoStep(flightContext);
    existingTableColumnNames =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, existingTable.getName());
    assertThat(
        "the first table column was removed from the existing BigQuery table view in the undo",
        !existingTableColumnNames.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was removed from the existing BigQuery table view in the undo",
        !existingTableColumnNames.containsKey(existingTableColumnB));

    Map<String, Field> existingRawTableColumnNames =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, existingTable.getRawTableName());
    assertThat(
        "the first table column was not removed from the existing BigQuery raw table in the undo",
        existingRawTableColumnNames.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was not removed from the existing BigQuery raw table in the undo",
        existingRawTableColumnNames.containsKey(existingTableColumnB));

    postgresColumnStep.undoStep(flightContext);
    Dataset postUndoUpdateDataset = datasetDao.retrieve(datasetId);
    existingTable = postUndoUpdateDataset.getTableByName(existingTableName).orElseThrow();
    existingTableColumnsMap = existingTable.getColumnsMap();
    assertThat(
        "the first table column was removed in the undo step",
        !existingTableColumnsMap.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was removed in the undo step",
        !existingTableColumnsMap.containsKey(existingTableColumnB));

    // Retry adding the same columns. Even though they already exist in the raw dataset table,
    // they will be re-added to the dataset view.
    postgresColumnStep.doStep(flightContext);
    bigQueryColumnStep.doStep(flightContext);
    existingTableColumnNames =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, existingTable.getName());
    assertThat(
        "the first table column was re-added to the existing BigQuery table",
        existingTableColumnNames.containsKey(existingTableColumnA));
    assertThat(
        "the second table column was re-added to the existing BigQuery table",
        existingTableColumnNames.containsKey(existingTableColumnB));

    bigQueryColumnStep.undoStep(flightContext);
    postgresColumnStep.undoStep(flightContext);

    // Retry adding a column with a different type
    updateModel
        .getChanges()
        .getAddColumns()
        .get(0)
        .setColumns(
            List.of(new ColumnModel().name(existingTableColumnA).datatype(TableDataType.INTEGER)));
    postgresColumnStep.doStep(flightContext);
    assertThrows(
        PdaoException.class,
        () -> {
          bigQueryColumnStep.doStep(flightContext);
        });
  }

  @Test
  public void testNewTableAndColumnAdditionSteps() throws Exception {
    String newTableName = "added_table";
    String newTableColumnName = "added_table_column";
    String anotherNewColumnName = "another_added_table_column";

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
                                .tableName(newTableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(anotherNewColumnName)
                                            .datatype(TableDataType.STRING))))));

    DatasetSchemaUpdateAddTablesPostgresStep postgresTableStep =
        new DatasetSchemaUpdateAddTablesPostgresStep(datasetTableDao, datasetId, updateModel);
    DatasetSchemaUpdateAddTablesBigQueryStep bigQueryTableStep =
        new DatasetSchemaUpdateAddTablesBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);
    DatasetSchemaUpdateAddColumnsPostgresStep postgresColumnStep =
        new DatasetSchemaUpdateAddColumnsPostgresStep(datasetTableDao, datasetId, updateModel);
    DatasetSchemaUpdateAddColumnsBigQueryStep bigQueryColumnStep =
        new DatasetSchemaUpdateAddColumnsBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);

    List<Step> steps =
        List.of(postgresTableStep, bigQueryTableStep, postgresColumnStep, bigQueryColumnStep);

    FlightContext flightContext = mock(FlightContext.class);

    for (Step step : steps) {
      step.doStep(flightContext);
    }

    Dataset postUpdateDataset = datasetDao.retrieve(datasetId);
    DatasetTable newTable = postUpdateDataset.getTableByName(newTableName).orElseThrow();
    Map<String, Column> newTableColumnsMap = newTable.getColumnsMap();
    assertThat(
        "The new table was created with the new column",
        newTableColumnsMap.containsKey(newTableColumnName));

    String datasetName = summaryModel.getName();
    String bqDatasetName = BigQueryPdao.prefixName(datasetName);
    BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetDao, datasetName);
    Map<String, Field> newTableFields =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, newTable.getName());
    assertThat(
        "The new BigQuery table was created with its new column",
        newTableFields.containsKey(newTableColumnName));

    for (int i = steps.size() - 1; i >= 0; i--) {
      Step step = steps.get(i);
      step.undoStep(flightContext);
    }

    Dataset postUndoUpdateDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "the new table does not exist after the undo step",
        postUndoUpdateDataset.getTableByName(newTableName).isEmpty());

    List<String> newBigQueryTableNames = newTable.getBigQueryTableNames();
    for (String postUndoBigQueryTable : newBigQueryTableNames) {
      assertFalse(
          String.format("the new table %s does not exist in BigQuery", postUndoBigQueryTable),
          bigQueryDatasetPdao.tableExists(postUpdateDataset, postUndoBigQueryTable));
    }
  }

  @Test
  public void testRepeatedColumnAdditionSteps() throws Exception {
    String newTableName = "added_table";
    String newTableColumnName = "repeated_column";
    String anotherNewColumnName = "another_repeated_column";

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("repeated column tests")
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
                                            .datatype(TableDataType.STRING)
                                            .arrayOf(true)))))
                    .addColumns(
                        List.of(
                            new DatasetSchemaColumnUpdateModel()
                                .tableName(newTableName)
                                .columns(
                                    List.of(
                                        new ColumnModel()
                                            .name(anotherNewColumnName)
                                            .datatype(TableDataType.STRING)
                                            .arrayOf(true))))));

    DatasetSchemaUpdateAddTablesPostgresStep postgresTableStep =
        new DatasetSchemaUpdateAddTablesPostgresStep(datasetTableDao, datasetId, updateModel);
    DatasetSchemaUpdateAddTablesBigQueryStep bigQueryTableStep =
        new DatasetSchemaUpdateAddTablesBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);
    DatasetSchemaUpdateAddColumnsPostgresStep postgresColumnStep =
        new DatasetSchemaUpdateAddColumnsPostgresStep(datasetTableDao, datasetId, updateModel);
    DatasetSchemaUpdateAddColumnsBigQueryStep bigQueryColumnStep =
        new DatasetSchemaUpdateAddColumnsBigQueryStep(
            bigQueryDatasetPdao, datasetDao, datasetId, updateModel);

    List<Step> steps =
        List.of(postgresTableStep, bigQueryTableStep, postgresColumnStep, bigQueryColumnStep);

    FlightContext flightContext = mock(FlightContext.class);

    for (Step step : steps) {
      step.doStep(flightContext);
    }

    Dataset postUpdateDataset = datasetDao.retrieve(datasetId);
    DatasetTable newTable = postUpdateDataset.getTableByName(newTableName).orElseThrow();
    Map<String, Column> newTableColumnsMap = newTable.getColumnsMap();
    assertTrue(
        "The new table was created with the new repeated column",
        newTableColumnsMap.get(newTableColumnName).isArrayOf());
    assertTrue(
        "The new table contains the second repeated column",
        newTableColumnsMap.get(anotherNewColumnName).isArrayOf());

    String datasetName = summaryModel.getName();
    String bqDatasetName = BigQueryPdao.prefixName(datasetName);
    BigQueryProject bigQueryProject = bigQueryProjectForDatasetName(datasetDao, datasetName);
    Map<String, Field> newTableFields =
        getBigQueryFieldsMap(bigQueryProject, bqDatasetName, newTable.getName());
    assertThat(
        "The array_of column created with the new table has the REPEATED mode",
        newTableFields.get(newTableColumnName).getMode().equals(Field.Mode.REPEATED));
    assertThat(
        "The second array_of column added after table creation has the REPEATED mode",
        newTableFields.get(anotherNewColumnName).getMode().equals(Field.Mode.REPEATED));

    for (int i = steps.size() - 1; i >= 0; i--) {
      Step step = steps.get(i);
      step.undoStep(flightContext);
    }

    Dataset postUndoUpdateDataset = datasetDao.retrieve(datasetId);
    assertThat(
        "the new table does not exist after the undo step",
        postUndoUpdateDataset.getTableByName(newTableName).isEmpty());

    List<String> newBigQueryTableNames = newTable.getBigQueryTableNames();
    for (String postUndoBigQueryTable : newBigQueryTableNames) {
      assertFalse(
          String.format("the new table %s does not exist in BigQuery", postUndoBigQueryTable),
          bigQueryDatasetPdao.tableExists(postUpdateDataset, postUndoBigQueryTable));
    }
  }

  private RelationshipModel testRelationshipModel(String relationshipName) {
    return new RelationshipModel()
        .name(relationshipName)
        .from(new RelationshipTermModel().table("thetable").column("thecolumn"))
        .to(new RelationshipTermModel().table("anothertable").column("anothercolumn"));
  }

  @Test
  public void testRelationshipAdditionSteps() throws Exception {
    String relationshipName = UUID.randomUUID().toString();
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("Connected test relationship addition")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addRelationships(List.of(testRelationshipModel(relationshipName))));

    FlightContext flightContext = mock(FlightContext.class);
    DatasetSchemaUpdateAddRelationshipsPostgresStep postgresStep =
        new DatasetSchemaUpdateAddRelationshipsPostgresStep(
            datasetTableDao, datasetId, relationshipDao, updateModel);

    postgresStep.doStep(flightContext);

    Dataset postUpdateDataset = datasetDao.retrieve(datasetId);
    List<String> postUpdateRelationships =
        postUpdateDataset.getRelationships().stream().map(Relationship::getName).toList();
    assertTrue("New relationship was created", postUpdateRelationships.contains(relationshipName));
  }

  private Map<String, Field> getBigQueryFieldsMap(
      BigQueryProject bigQueryProject, String bqDatasetName, String bqTableName) {
    String bigQueryProjectId = bigQueryProject.getProjectId();
    BigQuery bigQuery = bigQueryProject.getBigQuery();
    Table bigQueryTable =
        bigQuery.getTable(TableId.of(bigQueryProjectId, bqDatasetName, bqTableName));
    Map<String, Field> fieldsMap = new HashMap<>();
    bigQueryTable
        .getDefinition()
        .getSchema()
        .getFields()
        .forEach(field -> fieldsMap.put(field.getName(), field));
    return Collections.unmodifiableMap(fieldsMap);
  }
}
