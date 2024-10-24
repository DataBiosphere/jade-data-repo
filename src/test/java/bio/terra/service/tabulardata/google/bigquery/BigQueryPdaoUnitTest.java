package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_COUNT_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;
import static bio.terra.service.tabulardata.google.bigquery.BigQueryPdao.prefixName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWithIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.BQTestUtils;
import bio.terra.common.DateTimeUtils;
import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.filedata.exception.TooManyDmlStatementsOutstandingException;
import bio.terra.service.filedata.google.bq.BigQueryConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.stringtemplate.v4.ST;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class BigQueryPdaoUnitTest {

  @Captor private ArgumentCaptor<String> stringCaptor;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID PROFILE_1_ID = UUID.randomUUID();
  private static final String SNAPSHOT_NAME = "snapshotName";
  private static final String SNAPSHOT_PROJECT_ID = "snapshot_data";
  private static final String DATASET_PROJECT_ID = "dataset_data";
  private static final Instant SNAPSHOT_CREATION = Instant.now();
  private static final String DATASET_NAME = "datasetName";
  private static final String SNAPSHOT_DESCRIPTION = "snapshotDescription";
  private static final String TABLE_1_NAME = "tableA";
  private static final UUID TABLE_1_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_1_ID = UUID.randomUUID();
  private static final String TABLE_1_COL1_NAME = "col1a";
  private static final UUID TABLE_1_COL1_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_1_COL1_ID = UUID.randomUUID();
  private static final String TABLE_1_COL2_NAME = "col2a";
  private static final UUID TABLE_1_COL2_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_1_COL2_ID = UUID.randomUUID();

  private static final String TABLE_2_NAME = "tableB";
  private static final UUID TABLE_2_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_2_ID = UUID.randomUUID();
  private static final String TABLE_2_COL1_NAME = "col1b";
  private static final UUID TABLE_2_COL1_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_2_COL1_ID = UUID.randomUUID();
  private static final String TABLE_2_COL2_NAME = "col2b";
  private static final UUID TABLE_2_COL2_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_2_COL2_ID = UUID.randomUUID();
  private static final String TABLE_2_COL3_NAME = "col3b";
  private static final UUID TABLE_2_COL3_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_TABLE_2_COL3_ID = UUID.randomUUID();

  private static final Instant CREATED_AT = Instant.parse("2022-01-01T00:00:00.00Z");
  private static final long CREATED_AT_MICROS = DateTimeUtils.toEpochMicros(CREATED_AT);

  @Mock private BigQueryProject bigQueryProjectSnapshot;
  @Mock private BigQuery bigQuerySnapshot;
  @Mock private BigQueryProject bigQueryProjectDataset;

  private Snapshot snapshot;
  private BigQueryDatasetPdao bigQueryDatasetPdao;
  private BigQuerySnapshotPdao bigQuerySnapshotPdao;

  @BeforeEach
  void setUp() {

    when(bigQueryProjectSnapshot.getProjectId()).thenReturn(SNAPSHOT_PROJECT_ID);
    BigQueryProject.put(bigQueryProjectSnapshot);

    when(bigQueryProjectDataset.getProjectId()).thenReturn(DATASET_PROJECT_ID);
    BigQueryProject.put(bigQueryProjectDataset);

    bigQueryDatasetPdao = new BigQueryDatasetPdao();
    bigQuerySnapshotPdao =
        new BigQuerySnapshotPdao(
            mock(ApplicationConfiguration.class), mock(BigQueryConfiguration.class));
    snapshot = mockSnapshot();
  }

  private void mockGetBigQuery() {
    when(bigQueryProjectSnapshot.getBigQuery()).thenReturn(bigQuerySnapshot);
  }

  @Test
  void testMergeStagingHistoryError() {
    Dataset dataset = mockDataset();
    String flightId = "flightId";
    ST sqlTemplate = new ST(BigQueryDatasetPdao.mergeLoadHistoryStagingTableTemplate);
    sqlTemplate.add("project", bigQueryProjectDataset.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
    sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + flightId);
    sqlTemplate.add("loadTable", PDAO_LOAD_HISTORY_TABLE);
    String query = sqlTemplate.render();

    Throwable cause =
        new BigQueryException(
            HttpStatus.BAD_REQUEST.value(), "Too many DML statements outstanding against table");
    BQTestUtils.mockBQQueryError(
        bigQueryProjectDataset, query, new PdaoException("Failure executing query", cause));
    assertThrows(
        TooManyDmlStatementsOutstandingException.class,
        () -> bigQueryDatasetPdao.mergeStagingLoadHistoryTable(dataset, flightId));
  }

  @Test
  void testAddRowIdsToStagingTableUnsetExisting() throws InterruptedException {
    Dataset dataset = mockDataset();

    bigQueryDatasetPdao.addRowIdsToStagingTable(dataset, TABLE_1_NAME, true);

    verify(bigQueryProjectDataset).query(stringCaptor.capture());
    String sql = stringCaptor.getValue().strip();
    assertThat(sql, containsString("SET datarepo_row_id = GENERATE_UUID()"));
    assertThat("All rows get a new row ID", sql, endsWithIgnoringCase("WHERE true"));
  }

  @Test
  void testAddRowIdsToStagingTableKeepExisting() throws InterruptedException {
    Dataset dataset = mockDataset();

    bigQueryDatasetPdao.addRowIdsToStagingTable(dataset, TABLE_1_NAME, false);

    verify(bigQueryProjectDataset).query(stringCaptor.capture());
    String sql = stringCaptor.getValue().strip();
    assertThat(sql, containsString("SET datarepo_row_id = GENERATE_UUID()"));
    assertThat(
        "Existing row IDs are kept", sql, endsWithIgnoringCase("WHERE datarepo_row_id IS NULL"));
  }

  @Test
  void testGetRefIds() throws InterruptedException {
    String value1 = "value1";
    String value2 = "value2";
    BQTestUtils.mockBQQuery(
        bigQueryProjectDataset,
        "SELECT "
            + TABLE_1_COL1_NAME
            + " "
            + "FROM `"
            + DATASET_PROJECT_ID
            + ".datarepo_"
            + DATASET_NAME
            + "."
            + TABLE_1_NAME
            + "`",
        Schema.of(Field.of(TABLE_1_COL1_NAME, LegacySQLTypeName.STRING)),
        List.of(Map.of(TABLE_1_COL1_NAME, value1), Map.of(TABLE_1_COL1_NAME, value2)));

    DatasetTable table = snapshot.getSourceDataset().getTables().get(0);
    assertThat(
        bigQueryDatasetPdao.getRefIds(
            snapshot.getSourceDataset(), table.getName(), table.getColumns().get(0)),
        equalTo(List.of(value1, value2)));
  }

  @Test
  void testGetSnapshotRefIds() throws InterruptedException {
    String value1 = "value1";
    String value2 = "value2";

    BQTestUtils.mockBQQuery(
        bigQueryProjectSnapshot,
        "SELECT "
            + TABLE_1_COL1_NAME
            + " "
            + "FROM `"
            + DATASET_PROJECT_ID
            + ".datarepo_"
            + DATASET_NAME
            + "."
            + TABLE_1_NAME
            + "` S, "
            + "`"
            + SNAPSHOT_PROJECT_ID
            + "."
            + SNAPSHOT_NAME
            + ".datarepo_row_ids` R "
            + "WHERE S.datarepo_row_id = R.datarepo_row_id AND "
            + "R.datarepo_table_id = '"
            + TABLE_1_ID
            + "'",
        Schema.of(Field.of(TABLE_1_COL1_NAME, LegacySQLTypeName.STRING)),
        List.of(Map.of(TABLE_1_COL1_NAME, value1), Map.of(TABLE_1_COL1_NAME, value2)));

    DatasetTable table = snapshot.getSourceDataset().getTables().get(0);
    assertThat(
        bigQuerySnapshotPdao.getSnapshotRefIds(
            snapshot.getSourceDataset(),
            snapshot,
            table.getName(),
            table.getId().toString(),
            table.getColumns().get(0)),
        equalTo(List.of(value1, value2)));
  }

  @Test
  void testMapValuesToRows() throws InterruptedException {
    String input1 = "input1";
    String input2 = "input2";

    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectDataset,
        "SELECT T.datarepo_row_id, V.input_value FROM ("
            + "SELECT input_value FROM UNNEST(['"
            + input1
            + "','"
            + input2
            + "']) AS input_value) AS V "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS T "
            + "ON V.input_value = CAST(T."
            + TABLE_1_COL1_NAME
            + " AS STRING)",
        Schema.of(
            Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
            Field.of("input_value", LegacySQLTypeName.STRING)),
        List.of(
            Map.of("datarepo_row_id", drRowId1, "input_value", input1),
            Map.of("datarepo_row_id", drRowId2, "input_value", input2)));

    assertThat(
        bigQuerySnapshotPdao.mapValuesToRows(
            snapshot.getFirstSnapshotSource(), List.of(input1, input2), CREATED_AT),
        samePropertyValuesAs(
            new RowIdMatch().addMatch(input1, drRowId1).addMatch(input2, drRowId2)));
  }

  @Test
  void testMapValuesToRowsWithMismatch() throws InterruptedException {
    String ipt1 = "input1";
    String ipt2 = "input2";
    String ipt3 = "input3";

    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectDataset,
        "SELECT T.datarepo_row_id, V.input_value FROM ("
            + "SELECT input_value FROM UNNEST(['"
            + ipt1
            + "','"
            + ipt2
            + "','"
            + ipt3
            + "']) AS input_value) AS V "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS T "
            + "ON V.input_value = CAST(T."
            + TABLE_1_COL1_NAME
            + " AS STRING)",
        Schema.of(
            Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
            Field.of("input_value", LegacySQLTypeName.STRING)),
        List.of(
            Map.of("datarepo_row_id", drRowId1, "input_value", ipt1),
            Map.of("datarepo_row_id", drRowId2, "input_value", ipt2),
            Map.of("input_value", ipt3)));

    // Check that mismatches are also identified
    assertThat(
        bigQuerySnapshotPdao.mapValuesToRows(
            snapshot.getFirstSnapshotSource(), List.of(ipt1, ipt2, ipt3), CREATED_AT),
        samePropertyValuesAs(
            new RowIdMatch().addMatch(ipt1, drRowId1).addMatch(ipt2, drRowId2).addMismatch(ipt3)));
  }

  @Test
  void testCreateSnapshotEmptyRowIds() throws InterruptedException {
    mockGetBigQuery();
    mockNumRowIds(snapshot, TABLE_1_NAME, 0);

    bigQuerySnapshotPdao.createSnapshot(snapshot, List.of(), CREATED_AT);

    verify(bigQueryProjectSnapshot)
        .createDataset(SNAPSHOT_NAME, SNAPSHOT_DESCRIPTION, GoogleRegion.NORTHAMERICA_NORTHEAST1);

    // Note: explicitly building up sql to make it easier to verify
    verify(bigQuerySnapshot)
        .create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_1_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, "
                        + TABLE_1_COL1_NAME
                        + ","
                        + TABLE_1_COL2_NAME
                        + " "
                        + "FROM (SELECT S.datarepo_row_id, "
                        + TABLE_1_COL1_NAME
                        + ","
                        + TABLE_1_COL2_NAME
                        + " "
                        + "FROM `"
                        + DATASET_PROJECT_ID
                        + ".datarepo_"
                        + DATASET_NAME
                        + "."
                        + TABLE_1_NAME
                        + "` S, "
                        + "`"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids` R "
                        + "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = "
                        + "'"
                        + TABLE_1_ID
                        + "')")));

    verify(bigQuerySnapshot)
        .create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_2_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, "
                        + TABLE_2_COL1_NAME
                        + ","
                        + TABLE_2_COL2_NAME
                        + ","
                        + TABLE_2_COL3_NAME
                        + " "
                        + "FROM (SELECT S.datarepo_row_id, "
                        + TABLE_2_COL2_NAME
                        + " AS "
                        + TABLE_2_COL1_NAME
                        + ","
                        + TABLE_2_COL1_NAME
                        + " AS "
                        + TABLE_2_COL2_NAME
                        + ","
                        + "NULL AS "
                        + TABLE_2_COL3_NAME
                        + " "
                        + "FROM `"
                        + DATASET_PROJECT_ID
                        + ".datarepo_"
                        + DATASET_NAME
                        + "."
                        + TABLE_2_NAME
                        + "` S, "
                        + "`"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids` R "
                        + "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = "
                        + "'"
                        + TABLE_2_ID
                        + "')")));
  }

  @Test
  void testCreateSnapshotMismatchedRowIdCounts() throws InterruptedException {
    mockGetBigQuery();
    mockNumRowIds(snapshot, TABLE_1_NAME, 0);
    doReturn(null).when(bigQueryProjectSnapshot).query(anyString());

    List<String> rowIds = List.of(UUID.randomUUID().toString());
    assertThrows(
        PdaoException.class,
        () -> bigQuerySnapshotPdao.createSnapshot(snapshot, rowIds, CREATED_AT),
        "Invalid row ids supplied");
  }

  @Test
  void testCreateSnapshotWithRowIds() throws InterruptedException {
    mockGetBigQuery();
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);
    DatasetTable table2 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(1);
    String rootTblId =
        snapshot
            .getFirstSnapshotSource()
            .getAssetSpecification()
            .getRootTable()
            .getTable()
            .getId()
            .toString();
    mockNumRowIds(snapshot, TABLE_1_NAME, 2);

    // Verify that the rowIds are properly copied
    doReturn(null)
        .when(bigQueryProjectSnapshot)
        .query(
            "INSERT INTO `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_row_ids` "
                + "(datarepo_table_id,datarepo_row_id) "
                + "SELECT '"
                + rootTblId
                + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM ("
                + "SELECT row_id FROM UNNEST(['"
                + drRowId1
                + "','"
                + drRowId2
                + "']) AS row_id"
                + ") AS T");

    bigQuerySnapshotPdao.createSnapshot(snapshot, List.of(drRowId1, drRowId2), CREATED_AT);

    verify(bigQueryProjectSnapshot)
        .createDataset(SNAPSHOT_NAME, SNAPSHOT_DESCRIPTION, GoogleRegion.NORTHAMERICA_NORTHEAST1);

    // Verify the result of the relationship walk are written to the rowid table
    verify(bigQuerySnapshot)
        .query(
            QueryJobConfiguration.newBuilder(
                    "WITH merged_table AS (SELECT DISTINCT '"
                        + TABLE_2_ID
                        + "' AS datarepo_table_id, "
                        + "T.datarepo_row_id "
                        + "FROM (("
                        + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                            DATASET_PROJECT_ID, prefixName(DATASET_NAME), table2, null, CREATED_AT)
                        + ")) T, "
                        + "(("
                        + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                            DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
                        + ")) F, "
                        + "`"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids` R "
                        + "WHERE R.datarepo_table_id = '"
                        + TABLE_1_ID
                        + "' AND "
                        + "R.datarepo_row_id = F.datarepo_row_id AND F."
                        + TABLE_1_COL1_NAME
                        + " = "
                        + "T."
                        + TABLE_2_COL1_NAME
                        + ") "
                        + "SELECT datarepo_table_id,datarepo_row_id FROM merged_table WHERE "
                        + "datarepo_row_id NOT IN (SELECT datarepo_row_id "
                        + "FROM `"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids`)")
                .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_row_ids"))
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .setNamedParameters(
                    Map.of(
                        "transactionTerminatedAt",
                        QueryParameterValue.timestamp(CREATED_AT_MICROS)))
                .build());

    verify(bigQuerySnapshot)
        .create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_1_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, "
                        + TABLE_1_COL1_NAME
                        + ","
                        + TABLE_1_COL2_NAME
                        + " "
                        + "FROM (SELECT S.datarepo_row_id, "
                        + TABLE_1_COL1_NAME
                        + ","
                        + TABLE_1_COL2_NAME
                        + " "
                        + "FROM `"
                        + DATASET_PROJECT_ID
                        + ".datarepo_"
                        + DATASET_NAME
                        + "."
                        + TABLE_1_NAME
                        + "` S, "
                        + "`"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids` R "
                        + "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = "
                        + "'"
                        + TABLE_1_ID
                        + "')")));

    verify(bigQuerySnapshot)
        .create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_2_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, "
                        + TABLE_2_COL1_NAME
                        + ","
                        + TABLE_2_COL2_NAME
                        + ","
                        + TABLE_2_COL3_NAME
                        + " "
                        + "FROM (SELECT S.datarepo_row_id, "
                        + TABLE_2_COL2_NAME
                        + " AS "
                        + TABLE_2_COL1_NAME
                        + ","
                        + TABLE_2_COL1_NAME
                        + " AS "
                        + TABLE_2_COL2_NAME
                        + ","
                        + "NULL AS "
                        + TABLE_2_COL3_NAME
                        + " "
                        + "FROM `"
                        + DATASET_PROJECT_ID
                        + ".datarepo_"
                        + DATASET_NAME
                        + "."
                        + TABLE_2_NAME
                        + "` S, "
                        + "`"
                        + SNAPSHOT_PROJECT_ID
                        + "."
                        + SNAPSHOT_NAME
                        + ".datarepo_row_ids` R "
                        + "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = "
                        + "'"
                        + TABLE_2_ID
                        + "')")));
  }

  @Test
  void testCreateSnapshotWithLiveViews() throws InterruptedException {
    mockGetBigQuery();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);
    DatasetTable table2 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(1);

    // Make sure that validation is called and returns
    BQTestUtils.mockBQQuery(
        bigQueryProjectSnapshot,
        "SELECT datarepo_row_id FROM `"
            + SNAPSHOT_PROJECT_ID
            + "."
            + SNAPSHOT_NAME
            + ".datarepo_row_ids` "
            + "LIMIT 1",
        Schema.of(Field.of("cnt", LegacySQLTypeName.NUMERIC)),
        List.of(Map.of("cnt", UUID.randomUUID().toString())));

    // Verify call by mocking; unused stubs are an error.
    doReturn(mock(TableResult.class))
        .when(bigQueryProjectSnapshot)
        .query(
            "INSERT INTO `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_row_ids` "
                + "(datarepo_table_id, datarepo_row_id) "
                + "(SELECT '"
                + TABLE_1_ID
                + "', datarepo_row_id "
                + "FROM ("
                + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                    DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
                + ") AS L) "
                + "UNION ALL "
                + "(SELECT '"
                + TABLE_2_ID
                + "', datarepo_row_id "
                + "FROM ("
                + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                    DATASET_PROJECT_ID, prefixName(DATASET_NAME), table2, null, CREATED_AT)
                + ") AS L)",
            Map.of("transactionTerminatedAt", QueryParameterValue.timestamp(CREATED_AT_MICROS)));

    bigQuerySnapshotPdao.createSnapshotWithLiveViews(
        snapshot, snapshot.getSourceDataset(), CREATED_AT);

    // Make sure that rowId table is created
    verify(bigQueryProjectSnapshot)
        .createTable(
            snapshot.getName(),
            "datarepo_row_ids",
            Schema.of(
                Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING),
                Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING)));
  }

  @Test
  void testCreateSnapshotWithLiveViewsValidationFails() throws InterruptedException {
    // Make validation return that no records will be in snapshot
    BQTestUtils.mockBQQuery(
        bigQueryProjectSnapshot,
        "SELECT datarepo_row_id FROM `%s.%s.datarepo_row_ids` LIMIT 1"
            .formatted(SNAPSHOT_PROJECT_ID, SNAPSHOT_NAME),
        Schema.of(Field.of("cnt", LegacySQLTypeName.NUMERIC)),
        List.of());

    doReturn(mock(TableResult.class)).when(bigQueryProjectSnapshot).query(anyString(), any());

    Dataset sourceDataset = snapshot.getSourceDataset();
    assertThrows(
        PdaoException.class,
        () -> bigQuerySnapshotPdao.createSnapshotWithLiveViews(snapshot, sourceDataset, CREATED_AT),
        "This snapshot is empty");
  }

  @Test
  void testQueryForRowIds() throws InterruptedException {
    mockGetBigQuery();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    String query =
        "SELECT datarepo_row_id "
            + "FROM ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") "
            + "WHERE "
            + TABLE_1_COL2_NAME
            + " = 'abc'";

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setNamedParameters(
                Map.of("transactionTerminatedAt", QueryParameterValue.timestamp(CREATED_AT_MICROS)))
            .build();
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();

    // Mock query that is passed in
    BQTestUtils.mockBQQuery(
        bigQuerySnapshot,
        queryConfig,
        Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
        List.of(Map.of("datarepo_row_id", drRowId1), Map.of("datarepo_row_id", drRowId2)));

    // Mock validation query
    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectSnapshot,
        "SELECT COUNT(*) FROM `"
            + SNAPSHOT_PROJECT_ID
            + "."
            + SNAPSHOT_NAME
            + ".datarepo_temp` AS T "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS D "
            + "USING ( datarepo_row_id ) "
            + "WHERE D.datarepo_row_id IS NULL",
        Schema.of(Field.of("cnt", LegacySQLTypeName.STRING)),
        List.of(Map.of("cnt", "0")));

    AssetSpecification assetSpecification =
        snapshot.getFirstSnapshotSource().getAssetSpecification();
    AssetTable rootTable = assetSpecification.getRootTable();
    bigQuerySnapshotPdao.createSnapshotByQuery(assetSpecification, snapshot, query, CREATED_AT);

    verify(bigQueryProjectSnapshot)
        .query(
            "INSERT INTO `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_row_ids` "
                + "(datarepo_table_id,datarepo_row_id) "
                + "SELECT '"
                + rootTable.getTable().getId()
                + "' AS datarepo_table_id, T.row_id AS datarepo_row_id "
                + "FROM (SELECT datarepo_row_id AS row_id "
                + "FROM `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_temp` ) AS T");
  }

  @Test
  void testQueryForRowIdsQueryIsEmpty() {
    mockGetBigQuery();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    String query =
        "SELECT datarepo_row_id "
            + "FROM ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") "
            + "WHERE "
            + TABLE_1_NAME
            + "."
            + TABLE_1_COL2_NAME
            + " = 'abc'";

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setNamedParameters(
                Map.of("transactionTerminatedAt", QueryParameterValue.timestamp(CREATED_AT_MICROS)))
            .build();

    // Mock query that is passed in
    BQTestUtils.mockBQQuery(
        bigQuerySnapshot,
        queryConfig,
        Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
        List.of());
    AssetSpecification assetSpecification =
        snapshot.getFirstSnapshotSource().getAssetSpecification();
    assertThrows(
        InvalidQueryException.class,
        () ->
            bigQuerySnapshotPdao.createSnapshotByQuery(
                assetSpecification, snapshot, query, CREATED_AT),
        "Query returned 0 results");
  }

  @Test
  void testQueryForRowIdsValidationFails() {
    mockGetBigQuery();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    String query =
        "SELECT datarepo_row_id "
            + "FROM ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") "
            + "WHERE "
            + TABLE_1_COL2_NAME
            + " = 'abc'";

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setNamedParameters(
                Map.of("transactionTerminatedAt", QueryParameterValue.timestamp(CREATED_AT_MICROS)))
            .build();
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();

    // Mock query that is passed in
    BQTestUtils.mockBQQuery(
        bigQuerySnapshot,
        queryConfig,
        Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
        List.of(Map.of("datarepo_row_id", drRowId1), Map.of("datarepo_row_id", drRowId2)));

    // Mock validation query
    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectSnapshot,
        "SELECT COUNT(*) FROM `"
            + SNAPSHOT_PROJECT_ID
            + "."
            + SNAPSHOT_NAME
            + ".datarepo_temp` AS T "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS D "
            + "USING ( datarepo_row_id ) "
            + "WHERE D.datarepo_row_id IS NULL",
        Schema.of(Field.of("cnt", LegacySQLTypeName.STRING)),
        List.of(Map.of("cnt", "1")));

    AssetSpecification assetSpecification =
        snapshot.getFirstSnapshotSource().getAssetSpecification();
    assertThrows(
        MismatchedValueException.class,
        () ->
            bigQuerySnapshotPdao.createSnapshotByQuery(
                assetSpecification, snapshot, query, CREATED_AT),
        "Query results did not match dataset root row ids");
  }

  @Test
  void testMatchRowIds() throws InterruptedException {
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);

    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectDataset,
        "SELECT T.datarepo_row_id, V.input_value FROM ("
            + "SELECT input_value FROM UNNEST(['"
            + drRowId1
            + "','"
            + drRowId2
            + "']) AS input_value) AS V "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS T "
            + "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
        Schema.of(
            Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
            Field.of("input_value", LegacySQLTypeName.STRING)),
        List.of(
            Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
            Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2)));

    assertThat(
        bigQuerySnapshotPdao.matchRowIds(
            snapshot.getFirstSnapshotSource(),
            TABLE_1_NAME,
            List.of(UUID.fromString(drRowId1), UUID.fromString(drRowId2)),
            CREATED_AT),
        samePropertyValuesAs(
            new RowIdMatch().addMatch(drRowId1, drRowId1).addMatch(drRowId2, drRowId2)));
  }

  @Test
  void testMatchRowIdsWithMismatch() throws InterruptedException {
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();
    String drRowId3 = UUID.randomUUID().toString();
    DatasetTable table1 = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);
    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectDataset,
        "SELECT T.datarepo_row_id, V.input_value FROM ("
            + "SELECT input_value FROM UNNEST(['"
            + drRowId1
            + "','"
            + drRowId2
            + "','"
            + drRowId3
            + "']) AS "
            + "input_value) AS V "
            + "LEFT JOIN ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                DATASET_PROJECT_ID, prefixName(DATASET_NAME), table1, null, CREATED_AT)
            + ") AS T "
            + "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
        Schema.of(
            Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
            Field.of("input_value", LegacySQLTypeName.STRING)),
        List.of(
            Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
            Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2),
            Map.of("input_value", drRowId3)));

    assertThat(
        bigQuerySnapshotPdao.matchRowIds(
            snapshot.getFirstSnapshotSource(),
            TABLE_1_NAME,
            List.of(
                UUID.fromString(drRowId1), UUID.fromString(drRowId2), UUID.fromString(drRowId3)),
            CREATED_AT),
        samePropertyValuesAs(
            new RowIdMatch()
                .addMatch(drRowId1, drRowId1)
                .addMatch(drRowId2, drRowId2)
                .addMismatch(drRowId3)));
  }

  @Test
  void testCreateSnapshotWithProvidedIds() throws InterruptedException {
    mockGetBigQuery();
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();

    mockNumRowIds(snapshot, TABLE_2_NAME, 2);

    // Verify that the rowIds are properly copied
    doReturn(null)
        .when(bigQueryProjectSnapshot)
        .query(
            "INSERT INTO `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_row_ids` "
                + "(datarepo_table_id,datarepo_row_id) "
                + "SELECT '"
                + TABLE_2_ID
                + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM ("
                + "SELECT row_id FROM UNNEST(['"
                + drRowId1
                + "','"
                + drRowId2
                + "']) AS row_id"
                + ") AS T");

    SnapshotRequestContentsModel requestModel =
        new SnapshotRequestContentsModel()
            .rowIdSpec(
                new SnapshotRequestRowIdModel()
                    .addTablesItem(
                        new SnapshotRequestRowIdTableModel()
                            .addRowIdsItem(UUID.fromString(drRowId1))
                            .addRowIdsItem(UUID.fromString(drRowId2))
                            .addColumnsItem(TABLE_2_COL1_NAME)
                            .addColumnsItem(TABLE_2_COL2_NAME)
                            .tableName(TABLE_2_NAME)));
    bigQuerySnapshotPdao.createSnapshotWithProvidedIds(snapshot, requestModel, CREATED_AT);
  }

  @Test
  void testCreateSnapshotWithProvidedIdsWithInvalidRowIds() throws InterruptedException {
    String drRowId1 = UUID.randomUUID().toString();
    String drRowId2 = UUID.randomUUID().toString();

    mockNumRowIds(snapshot, TABLE_2_NAME, 0);

    // Verify that the rowIds are properly copied (make sure that it still actually happens)
    doReturn(null)
        .when(bigQueryProjectSnapshot)
        .query(
            "INSERT INTO `"
                + SNAPSHOT_PROJECT_ID
                + "."
                + SNAPSHOT_NAME
                + ".datarepo_row_ids` "
                + "(datarepo_table_id,datarepo_row_id) "
                + "SELECT '"
                + TABLE_2_ID
                + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM ("
                + "SELECT row_id FROM UNNEST(['"
                + drRowId1
                + "','"
                + drRowId2
                + "']) AS row_id"
                + ") AS T");

    SnapshotRequestContentsModel requestModel =
        new SnapshotRequestContentsModel()
            .rowIdSpec(
                new SnapshotRequestRowIdModel()
                    .addTablesItem(
                        new SnapshotRequestRowIdTableModel()
                            .addRowIdsItem(UUID.fromString(drRowId1))
                            .addRowIdsItem(UUID.fromString(drRowId2))
                            .addColumnsItem(TABLE_2_COL1_NAME)
                            .addColumnsItem(TABLE_2_COL2_NAME)
                            .tableName(TABLE_2_NAME)));
    assertThrows(
        PdaoException.class,
        () ->
            bigQuerySnapshotPdao.createSnapshotWithProvidedIds(snapshot, requestModel, CREATED_AT),
        "Invalid row ids supplied");
  }

  @Test
  void aggregateSnapshotTableTest() {
    String stringTest = "hello";
    int intTest = 1234567;
    List<String> listTest = List.of("a", "b", "c");

    List<FieldValueList> listOfFieldValueList =
        List.of(
            FieldValueList.of(
                List.of(
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, stringTest),
                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, intTest),
                    FieldValue.of(
                        FieldValue.Attribute.REPEATED,
                        listTest.stream()
                            .map(s -> FieldValue.of(FieldValue.Attribute.PRIMITIVE, s))
                            .toList()))));

    List<StandardSQLTypeName> standardSQLTypeNames =
        List.of(StandardSQLTypeName.STRING, StandardSQLTypeName.INT64, StandardSQLTypeName.ARRAY);

    Schema schema =
        Schema.of(
            FieldList.of(
                standardSQLTypeNames.stream()
                    .map(
                        s -> {
                          Field.Builder fieldBuilder = Field.newBuilder(s.name(), s);
                          if (s == StandardSQLTypeName.ARRAY) {
                            fieldBuilder.setType(StandardSQLTypeName.STRING);
                          }
                          return fieldBuilder.build();
                        })
                    .toList()));

    Page<FieldValueList> page = mockPage(listOfFieldValueList);

    TableResult table =
        TableResult.newBuilder().setSchema(schema).setTotalRows(10L).setPageNoSchema(page).build();

    List<BigQueryDataResultModel> result = BigQueryPdao.aggregateTableData(table);

    assertEquals(stringTest, result.get(0).getRowResult().get("STRING"));
    assertEquals(intTest, result.get(0).getRowResult().get("INT64"));
    assertEquals(listTest, result.get(0).getRowResult().get("ARRAY"));
  }

  @Test
  void testAggregateTextColumnStats() {
    String columnName = "Column1";
    Schema schema =
        Schema.of(
            FieldList.of(
                List.of(
                    Field.newBuilder(columnName, StandardSQLTypeName.STRING).build(),
                    Field.newBuilder(PDAO_COUNT_COLUMN_NAME, StandardSQLTypeName.INT64).build())));

    Page<FieldValueList> page =
        mockPage(
            List.of(
                FieldValueList.of(
                    List.of(
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Column1Value"),
                        FieldValue.of(
                            FieldValue.Attribute.PRIMITIVE,
                            "2"))))); // For some reason, it wants the numeric value passed in as a
    // string

    TableResult table =
        TableResult.newBuilder().setSchema(schema).setTotalRows(10L).setPageNoSchema(page).build();

    List<ColumnStatisticsTextValue> result =
        BigQueryPdao.aggregateTextColumnStats(table, columnName);

    assertThat("ColumnStatisticsTextValue should be returned", result, hasSize(1));
    assertThat(
        "ColumnStatisticsTextValue should have the correct value",
        result.get(0).getValue(),
        equalTo("Column1Value"));
    assertThat(
        "ColumnStatisticsTextValue should have the correct count",
        result.get(0).getCount(),
        equalTo(2));
  }

  @Test
  void testBQDatasetFullyQualifiedTableName() {
    Dataset dataset = mockDataset();
    String tableName = "table";
    String expected =
        "`" + DATASET_PROJECT_ID + "." + PDAO_PREFIX + dataset.getName() + "." + tableName + "`";
    String actual = BigQueryPdao.bqFullyQualifiedTableName(dataset, tableName);
    assertThat("Dataset BQ table name is correctly formatted", expected, equalTo(actual));
  }

  @Test
  void testBQSnapshotFullyQualifiedTableName() {
    Snapshot snapshot = mockSnapshot();
    String tableName = "table";
    String expected = "`" + SNAPSHOT_PROJECT_ID + "." + snapshot.getName() + "." + tableName + "`";
    String actual = BigQueryPdao.bqFullyQualifiedTableName(snapshot, tableName);
    assertThat("Snapshot BQ table name is correctly formatted", expected, equalTo(actual));
  }

  @Test
  void testBQDatasetTableName() {
    Dataset dataset = mockDataset();
    String tableName = "table";
    String expected = PDAO_PREFIX + dataset.getName() + "." + tableName;
    String actual = BigQueryPdao.bqTableName(dataset, tableName);
    assertThat("Dataset BQ table name is correctly formatted", expected, equalTo(actual));
  }

  @Test
  void testBQSnapshotTableName() {
    Snapshot snapshot = mockSnapshot();
    String tableName = "table";
    String expected = snapshot.getName() + "." + tableName;
    String actual = BigQueryPdao.bqTableName(snapshot, tableName);
    assertThat("Snapshot BQ table name is correctly formatted", expected, equalTo(actual));
  }

  private Dataset mockDataset() {
    DatasetTable tbl1 =
        DatasetFixtures.generateDatasetTable(
                TABLE_1_NAME, TableDataType.STRING, List.of(TABLE_1_COL1_NAME, TABLE_1_COL2_NAME))
            .id(TABLE_1_ID);
    tbl1.getColumns().get(0).id(TABLE_1_COL1_ID);
    tbl1.getColumns().get(1).id(TABLE_1_COL2_ID);

    DatasetTable tbl2 =
        DatasetFixtures.generateDatasetTable(
                TABLE_2_NAME,
                TableDataType.STRING,
                List.of(TABLE_2_COL1_NAME, TABLE_2_COL2_NAME, TABLE_2_COL3_NAME))
            .id(TABLE_2_ID);
    tbl2.getColumns().get(0).id(TABLE_2_COL1_ID);
    tbl2.getColumns().get(1).id(TABLE_2_COL2_ID);
    tbl2.getColumns().get(2).id(TABLE_2_COL3_ID);

    return new Dataset()
        .id(DATASET_ID)
        .name(DATASET_NAME)
        .projectResource(
            new GoogleProjectResource().profileId(PROFILE_1_ID).googleProjectId(DATASET_PROJECT_ID))
        .tables(List.of(tbl1, tbl2))
        .storage(
            List.of(
                new GoogleStorageResource(
                    DATASET_ID,
                    GoogleCloudResource.BIGQUERY,
                    GoogleRegion.NORTHAMERICA_NORTHEAST1)));
  }

  private Snapshot mockSnapshot() {
    Dataset dataset = mockDataset();
    List<DatasetTable> tables = dataset.getTables();
    DatasetTable tbl1 = tables.get(0);
    DatasetTable tbl2 = tables.get(1);

    SnapshotTable snpTbl1 =
        new SnapshotTable().id(SNAPSHOT_TABLE_1_ID).name(tbl1.getName()).columns(tbl1.getColumns());
    snpTbl1.getColumns().get(0).id(SNAPSHOT_TABLE_1_COL1_ID);
    snpTbl1.getColumns().get(1).id(SNAPSHOT_TABLE_1_COL2_ID);

    SnapshotTable snpTbl2 =
        new SnapshotTable().id(SNAPSHOT_TABLE_2_ID).name(tbl2.getName()).columns(tbl2.getColumns());
    snpTbl2.getColumns().get(0).id(SNAPSHOT_TABLE_2_COL1_ID);
    snpTbl2.getColumns().get(1).id(SNAPSHOT_TABLE_2_COL2_ID);
    snpTbl2.getColumns().get(2).id(SNAPSHOT_TABLE_2_COL3_ID);

    return new Snapshot()
        .id(SNAPSHOT_ID)
        .name(SNAPSHOT_NAME)
        .description(SNAPSHOT_DESCRIPTION)
        .createdDate(SNAPSHOT_CREATION)
        .profileId(PROFILE_1_ID)
        .projectResource(
            new GoogleProjectResource()
                .profileId(PROFILE_1_ID)
                .googleProjectId(SNAPSHOT_PROJECT_ID))
        .snapshotTables(List.of(snpTbl1, snpTbl2))
        .snapshotSources(
            List.of(
                new SnapshotSource()
                    .dataset(mockDataset())
                    .setAssetSpecification(
                        new AssetSpecification()
                            .rootTable(new AssetTable().datasetTable(tbl1))
                            .rootColumn(
                                new AssetColumn()
                                    .datasetTable(tbl1)
                                    .datasetColumn(tbl1.getColumns().get(0)))
                            .assetRelationships(
                                List.of(
                                    new AssetRelationship()
                                        .datasetRelationship(
                                            new Relationship()
                                                .fromTable(tbl1)
                                                .fromColumn(tbl1.getColumns().get(0))
                                                .toTable(tbl2)
                                                .toColumn(tbl2.getColumns().get(0))))))
                    .snapshotMapTables(
                        List.of(
                            new SnapshotMapTable()
                                .fromTable(tbl1)
                                .toTable(snpTbl1)
                                .snapshotMapColumns(
                                    List.of(
                                        new SnapshotMapColumn()
                                            .fromColumn(tbl1.getColumns().get(0))
                                            .toColumn(snpTbl1.getColumns().get(0)),
                                        new SnapshotMapColumn()
                                            .fromColumn(tbl1.getColumns().get(1))
                                            .toColumn(snpTbl1.getColumns().get(1)))),
                            new SnapshotMapTable()
                                .fromTable(tbl2)
                                .toTable(snpTbl2)
                                // Note: purposefully not mapping the third column and swappign the
                                // first two\
                                // for special handling
                                .snapshotMapColumns(
                                    List.of(
                                        new SnapshotMapColumn()
                                            .fromColumn(tbl2.getColumns().get(0))
                                            .toColumn(snpTbl2.getColumns().get(1)),
                                        new SnapshotMapColumn()
                                            .fromColumn(tbl2.getColumns().get(1))
                                            .toColumn(snpTbl2.getColumns().get(0))))))));
  }

  private void mockNumRowIds(Snapshot snapshot, String tableName, int numRowIds) {
    String datasetProjectId = snapshot.getSourceDataset().getProjectResource().getGoogleProjectId();
    String datasetName = snapshot.getSourceDataset().getName();
    String snapshotProjectId = snapshot.getProjectResource().getGoogleProjectId();
    String snapshotName = snapshot.getName();
    DatasetTable table =
        snapshot.getFirstSnapshotSource().getDataset().getTableByName(tableName).orElseThrow();
    BQTestUtils.mockBQQueryWithArgs(
        bigQueryProjectSnapshot,
        "SELECT COUNT(1) "
            + "FROM ("
            + BigQueryDatasetPdao.renderDatasetLiveViewSql(
                datasetProjectId, prefixName(datasetName), table, null, CREATED_AT)
            + ") AS T, "
            + "`"
            + snapshotProjectId
            + "."
            + snapshotName
            + ".datarepo_row_ids` AS R "
            + "WHERE R.datarepo_row_id = T.datarepo_row_id",
        Schema.of(Field.of("val", LegacySQLTypeName.NUMERIC)),
        List.of(Map.of("val", Integer.toString(numRowIds))));
  }

  public static Page<FieldValueList> mockPage(List<FieldValueList> listOfFieldValueList) {
    return new Page<>() {
      @Override
      public boolean hasNextPage() {
        return false;
      }

      @Override
      public String getNextPageToken() {
        return "";
      }

      @Override
      public Page<FieldValueList> getNextPage() {
        return this;
      }

      @Override
      public Iterable<FieldValueList> iterateAll() {
        return listOfFieldValueList;
      }

      @Override
      public Iterable<FieldValueList> getValues() {
        return listOfFieldValueList;
      }
    };
  }
}
