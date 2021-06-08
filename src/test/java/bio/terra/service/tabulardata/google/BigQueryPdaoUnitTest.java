package bio.terra.service.tabulardata.google;


import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.BQTestUtils;
import bio.terra.common.Relationship;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.CloudPlatform;
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
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.filedata.google.bq.BigQueryConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Category(Unit.class)
public class BigQueryPdaoUnitTest {


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

    @Mock
    private ApplicationConfiguration applicationConfiguration;
    @Mock
    private BigQueryConfiguration bigQueryConfiguration;
    @Mock
    private BigQueryProject bigQueryProjectSnapshot;
    @Mock
    private BigQuery bigQuerySnapshot;
    @Mock
    private BigQueryProject bigQueryProjectDataset;
    @Mock
    private BigQuery bigQueryDataset;
    @Mock
    private BigQueryProjectProvider bigQueryProjectProvider;

    private Snapshot snapshot;
    private BigQueryPdao dao;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(bigQueryProjectSnapshot.getProjectId()).thenReturn(SNAPSHOT_PROJECT_ID);
        when(bigQueryProjectSnapshot.getBigQuery()).thenReturn(bigQuerySnapshot);
        when(bigQueryProjectProvider.apply(SNAPSHOT_PROJECT_ID)).thenReturn(bigQueryProjectSnapshot);

        when(bigQueryProjectDataset.getProjectId()).thenReturn(DATASET_PROJECT_ID);
        when(bigQueryProjectDataset.getBigQuery()).thenReturn(bigQueryDataset);
        when(bigQueryProjectProvider.apply(DATASET_PROJECT_ID)).thenReturn(bigQueryProjectDataset);

        dao = new BigQueryPdao(applicationConfiguration, bigQueryConfiguration, bigQueryProjectProvider);
        snapshot = mockSnapshot();
    }

    @Test
    public void testGetRefIds() throws InterruptedException {
        String value1 = "value1";
        String value2 = "value2";
        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT " + TABLE_1_COL1_NAME + " " +
                "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "`",
            Schema.of(Field.of(TABLE_1_COL1_NAME, LegacySQLTypeName.STRING)),
            List.of(
                Map.of(TABLE_1_COL1_NAME, value1),
                Map.of(TABLE_1_COL1_NAME, value2)));

        DatasetTable table = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);
        assertThat(
            dao.getRefIds(snapshot.getFirstSnapshotSource().getDataset(), table.getName(), table.getColumns().get(0)),
            equalTo(List.of(value1, value2))
        );
    }

    @Test
    public void testGetSnapshotRefIds() throws InterruptedException {
        String value1 = "value1";
        String value2 = "value2";
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT " + TABLE_1_COL1_NAME + " " +
                "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` S, " +
                "`" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                "WHERE S.datarepo_row_id = R.datarepo_row_id AND " +
                    "R.datarepo_table_id = '" +  TABLE_1_ID + "'",
            Schema.of(Field.of(TABLE_1_COL1_NAME, LegacySQLTypeName.STRING)),
            List.of(
                Map.of(TABLE_1_COL1_NAME, value1),
                Map.of(TABLE_1_COL1_NAME, value2)));

        DatasetTable table = snapshot.getFirstSnapshotSource().getDataset().getTables().get(0);
        assertThat(
            dao.getSnapshotRefIds(
                snapshot.getFirstSnapshotSource().getDataset(),
                snapshot,
                table.getName(),
                table.getId().toString(),
                table.getColumns().get(0)),
            equalTo(List.of(value1, value2))
        );
    }

    @Test
    public void testMapValuesToRows() throws InterruptedException {
        String input1 = "input1";
        String input2 = "input2";

        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + input1 + "','" + input2 + "']) AS input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T." + TABLE_1_COL1_NAME + " AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", input1),
                Map.of("datarepo_row_id", drRowId2, "input_value", input2)));

        assertThat(dao.mapValuesToRows(snapshot.getFirstSnapshotSource(), List.of(input1, input2)),
            samePropertyValuesAs(new RowIdMatch().addMatch(input1, drRowId1).addMatch(input2, drRowId2)));
    }

    @Test
    public void testMapValuesToRowsWithMismatch() throws InterruptedException {
        String ipt1 = "input1";
        String ipt2 = "input2";
        String ipt3 = "input3";

        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + ipt1 + "','" + ipt2 + "','" + ipt3 + "']) AS input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T." + TABLE_1_COL1_NAME + " AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", ipt1),
                Map.of("datarepo_row_id", drRowId2, "input_value", ipt2),
                Map.of("input_value", ipt3)));

        // Check that mismatches are also identified
        assertThat(dao.mapValuesToRows(snapshot.getFirstSnapshotSource(), List.of(ipt1, ipt2, ipt3)),
            samePropertyValuesAs(new RowIdMatch().addMatch(ipt1, drRowId1).addMatch(ipt2, drRowId2).addMismatch(ipt3)));
    }

    @Test
    public void testCreateSnapshotEmptyRowIds() throws InterruptedException {
        mockNumRowIds(snapshot, TABLE_1_NAME, 0);

        dao.createSnapshot(snapshot, Collections.emptyList());

        verify(bigQueryProjectSnapshot, times(1)).createDataset(
            SNAPSHOT_NAME, SNAPSHOT_DESCRIPTION, GoogleRegion.NORTHAMERICA_NORTHEAST1
        );

        // Note: explicitly building up sql to make it easier to verify
        verify(bigQuerySnapshot, times(1)).create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_1_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, " +
                        TABLE_1_COL1_NAME + "," +
                        TABLE_1_COL2_NAME + " " +
                        "FROM (SELECT S.datarepo_row_id, " +
                        TABLE_1_COL1_NAME + "," +
                        TABLE_1_COL2_NAME + " " +
                        "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` S, " +
                        "`" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                        "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = " +
                        "'" + TABLE_1_ID + "')")));

        verify(bigQuerySnapshot, times(1)).create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_2_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, " +
                        TABLE_2_COL1_NAME + "," +
                        TABLE_2_COL2_NAME + "," +
                        TABLE_2_COL3_NAME + " " +
                        "FROM (SELECT S.datarepo_row_id, " +
                        TABLE_2_COL2_NAME + " AS " + TABLE_2_COL1_NAME + "," +
                        TABLE_2_COL1_NAME + " AS " + TABLE_2_COL2_NAME + "," +
                        "NULL AS " + TABLE_2_COL3_NAME + " " +
                        "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_2_NAME + "` S, " +
                        "`" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                        "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = " +
                        "'" + TABLE_2_ID + "')")));
    }

    @Test
    public void testCreateSnapshotMismatchedRowIdCounts() throws InterruptedException {
        mockNumRowIds(snapshot, TABLE_1_NAME, 0);

        assertThrows(
            PdaoException.class,
            () -> dao.createSnapshot(snapshot, List.of(UUID.randomUUID().toString())),
            "Invalid row ids supplied");
    }

    @Test
    public void testCreateSnapshotWithRowIds() throws InterruptedException {
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();
        String rootTblId =
            snapshot.getFirstSnapshotSource().getAssetSpecification().getRootTable().getTable().getId().toString();
        mockNumRowIds(snapshot, TABLE_1_NAME, 2);

        dao.createSnapshot(snapshot, List.of(drRowId1,  drRowId2));

        verify(bigQueryProjectSnapshot, times(1)).createDataset(
            SNAPSHOT_NAME, SNAPSHOT_DESCRIPTION, GoogleRegion.NORTHAMERICA_NORTHEAST1
        );

        // Verify that the rowIds are properly copied
        verify(bigQueryProjectSnapshot, times(1)).query(
            "INSERT INTO `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "(datarepo_table_id,datarepo_row_id) " +
                "SELECT '" + rootTblId + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM (" +
                "SELECT row_id FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS row_id" +
                ") AS T");

        // Verify the result of the relationship walk are written to the rowid table
        verify(bigQuerySnapshot, times(1)).query(
            QueryJobConfiguration.newBuilder(
                "WITH merged_table AS (SELECT DISTINCT '" + TABLE_2_ID + "' AS datarepo_table_id, " +
                    "T.datarepo_row_id " +
                    "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_2_NAME + "` T, " +
                    "`" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` F, " +
                    "`" + SNAPSHOT_PROJECT_ID + "." +  SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                    "WHERE R.datarepo_table_id = '" + TABLE_1_ID + "' AND " +
                    "R.datarepo_row_id = F.datarepo_row_id AND T." + TABLE_2_COL1_NAME + " = " +
                    "F." + TABLE_1_COL1_NAME + ") " +
                    "SELECT datarepo_table_id,datarepo_row_id FROM merged_table WHERE " +
                    "datarepo_row_id NOT IN (SELECT datarepo_row_id " +
                    "FROM `" + SNAPSHOT_PROJECT_ID + "." +  SNAPSHOT_NAME + ".datarepo_row_ids`)")
                .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_row_ids"))
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .build());

        verify(bigQuerySnapshot, times(1)).create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_1_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, " +
                        TABLE_1_COL1_NAME + "," +
                        TABLE_1_COL2_NAME + " " +
                        "FROM (SELECT S.datarepo_row_id, " +
                        TABLE_1_COL1_NAME + "," +
                        TABLE_1_COL2_NAME + " " +
                        "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` S, " +
                        "`" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                        "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = " +
                        "'" + TABLE_1_ID + "')")));

        verify(bigQuerySnapshot, times(1)).create(
            TableInfo.of(
                TableId.of(SNAPSHOT_NAME, TABLE_2_NAME),
                ViewDefinition.of(
                    "SELECT datarepo_row_id, " +
                        TABLE_2_COL1_NAME + "," +
                        TABLE_2_COL2_NAME + "," +
                        TABLE_2_COL3_NAME + " " +
                        "FROM (SELECT S.datarepo_row_id, " +
                        TABLE_2_COL2_NAME + " AS " + TABLE_2_COL1_NAME + "," +
                        TABLE_2_COL1_NAME + " AS " + TABLE_2_COL2_NAME + "," +
                        "NULL AS " + TABLE_2_COL3_NAME + " " +
                        "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_2_NAME + "` S, " +
                        "`" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` R " +
                        "WHERE S.datarepo_row_id = R.datarepo_row_id AND R.datarepo_table_id = " +
                        "'" + TABLE_2_ID + "')")));
    }

    @Test
    public void testCreateSnapshotWithLiveViews() throws InterruptedException {
        // Make sure that validation is called and returns
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT datarepo_row_id FROM `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "LIMIT 1",
            Schema.of(Field.of("cnt", LegacySQLTypeName.NUMERIC)),
            List.of(Map.of("cnt", UUID.randomUUID().toString())));

        dao.createSnapshotWithLiveViews(snapshot, snapshot.getFirstSnapshotSource().getDataset());

        // Make sure that rowId table is created
        verify(bigQueryProjectSnapshot, times(1))
            .createTable(snapshot.getName(), "datarepo_row_ids", Schema.of(
                Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING),
                Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING)));

        // Make sure that rowIds are inserted
        verify(bigQueryProjectSnapshot, times(1)).
            query("INSERT INTO `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "(datarepo_table_id, datarepo_row_id) " +
                "(SELECT '" + TABLE_1_ID + "', datarepo_row_id " +
                "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "`) " +
                "UNION ALL " +
                "(SELECT '" + TABLE_2_ID + "', datarepo_row_id " +
                "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_2_NAME + "`)");
    }

    @Test
    public void testCreateSnapshotWithLiveViewsValidationFails() throws InterruptedException {
        // Make validation return that no records will be in snapshot
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT datarepo_row_id FROM `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "LIMIT 1",
            Schema.of(Field.of("cnt", LegacySQLTypeName.NUMERIC)),
            Collections.emptyList());

        assertThrows(
            PdaoException.class,
            () -> dao.createSnapshotWithLiveViews(snapshot, snapshot.getFirstSnapshotSource().getDataset()),
            "This snapshot is empty");
    }

    @Test
    public void testQueryForRowIds() throws InterruptedException {
        String query = "SELECT datarepo_row_id " +
            "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` " +
            "WHERE " + TABLE_1_COL2_NAME + " = 'abc'";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        // Mock query that is passed in
        BQTestUtils.mockBQQuery(
            bigQuerySnapshot,
            queryConfig,
            Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1),
                Map.of("datarepo_row_id", drRowId2)));

        // Mock validation query
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT COUNT(*) FROM `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_temp` AS T " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS D " +
                "USING ( datarepo_row_id ) " +
                "WHERE D.datarepo_row_id IS NULL",
            Schema.of(Field.of("cnt", LegacySQLTypeName.STRING)),
            List.of(Map.of("cnt", "0")));

        AssetSpecification assetSpecification = snapshot.getFirstSnapshotSource().getAssetSpecification();
        AssetTable rootTable = assetSpecification.getRootTable();
        dao.queryForRowIds(assetSpecification, snapshot, query);

        verify(bigQueryProjectSnapshot, times(1)).query(
            "INSERT INTO `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "(datarepo_table_id,datarepo_row_id) " +
                "SELECT '" + rootTable.getTable().getId() + "' AS datarepo_table_id, T.row_id AS datarepo_row_id " +
                "FROM (SELECT datarepo_row_id AS row_id " +
                "FROM `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_temp` ) AS T");
    }

    @Test
    public void testQueryForRowIdsQueryIsEmpty() throws InterruptedException {
        String query = "SELECT datarepo_row_id " +
            "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` " +
            "WHERE " + TABLE_1_NAME + "." + TABLE_1_COL2_NAME + " = 'abc'";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

        // Mock query that is passed in
        BQTestUtils.mockBQQuery(
            bigQuerySnapshot,
            queryConfig,
            Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
            Collections.emptyList());
        assertThrows(
            InvalidQueryException.class, () ->
            dao.queryForRowIds(snapshot.getFirstSnapshotSource().getAssetSpecification(), snapshot, query),
            "Query returned 0 results");
    }

    @Test
    public void testQueryForRowIdsValidationFails() throws InterruptedException {
        String query = "SELECT datarepo_row_id " +
            "FROM `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` " +
            "WHERE " + TABLE_1_COL2_NAME + " = 'abc'";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(TableId.of(SNAPSHOT_NAME, "datarepo_temp"))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        // Mock query that is passed in
        BQTestUtils.mockBQQuery(
            bigQuerySnapshot,
            queryConfig,
            Schema.of(Field.of("datarepo_row_id", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1),
                Map.of("datarepo_row_id", drRowId2)));

        // Mock validation query
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT COUNT(*) FROM `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_temp` AS T " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS D " +
                "USING ( datarepo_row_id ) " +
                "WHERE D.datarepo_row_id IS NULL",
            Schema.of(Field.of("cnt", LegacySQLTypeName.STRING)),
            List.of(Map.of("cnt", "1")));

        assertThrows(
            MismatchedValueException.class, () ->
            dao.queryForRowIds(snapshot.getFirstSnapshotSource().getAssetSpecification(), snapshot, query),
            "Query results did not match dataset root row ids");
    }

    @Test
    public void testMatchRowIds() throws InterruptedException {
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
                Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2)));

        assertThat(dao.matchRowIds(snapshot.getFirstSnapshotSource(), TABLE_1_NAME, List.of(drRowId1, drRowId2)),
            samePropertyValuesAs(new RowIdMatch().addMatch(drRowId1, drRowId1).addMatch(drRowId2, drRowId2)));
    }

    @Test
    public void testMatchRowIdsWithMismatch() throws InterruptedException {
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();
        String drRowId3 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "','" + drRowId3 + "']) AS " +
                "input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
                Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2),
                Map.of("input_value", drRowId3)));

        assertThat(
            dao.matchRowIds(snapshot.getFirstSnapshotSource(), TABLE_1_NAME, List.of(drRowId1, drRowId2, drRowId3)),
            samePropertyValuesAs(new RowIdMatch()
                .addMatch(drRowId1, drRowId1)
                .addMatch(drRowId2, drRowId2)
                .addMismatch(drRowId3)));
    }

    @Test
    public void testCreateSnapshotWithProvidedIds() throws InterruptedException {
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
                Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2)));

        mockNumRowIds(snapshot, TABLE_2_NAME, 2);

        SnapshotRequestContentsModel requestModel = new SnapshotRequestContentsModel()
            .rowIdSpec(new SnapshotRequestRowIdModel()
                .addTablesItem(new SnapshotRequestRowIdTableModel()
                    .addRowIdsItem(drRowId1).addRowIdsItem(drRowId2)
                    .addColumnsItem(TABLE_2_COL1_NAME)
                    .addColumnsItem(TABLE_2_COL2_NAME)
                    .tableName(TABLE_2_NAME)));
        dao.createSnapshotWithProvidedIds(snapshot, requestModel);

        // Verify that the rowIds are properly copied
        verify(bigQueryProjectSnapshot, times(1)).query(
            "INSERT INTO `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "(datarepo_table_id,datarepo_row_id) " +
                "SELECT '" + TABLE_2_ID + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM (" +
                "SELECT row_id FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS row_id" +
                ") AS T");
    }

    @Test
    public void testCreateSnapshotWithProvidedIdsWithInvalidRowIds() throws InterruptedException {
        String drRowId1 = UUID.randomUUID().toString();
        String drRowId2 = UUID.randomUUID().toString();

        BQTestUtils.mockBQQuery(
            bigQueryProjectDataset,
            "SELECT T.datarepo_row_id, V.input_value FROM (" +
                "SELECT input_value FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS input_value) AS V " +
                "LEFT JOIN `" + DATASET_PROJECT_ID + ".datarepo_" + DATASET_NAME + "." + TABLE_1_NAME + "` AS T " +
                "ON V.input_value = CAST(T.datarepo_row_id AS STRING)",
            Schema.of(
                Field.of("datarepo_row_id", LegacySQLTypeName.STRING),
                Field.of("input_value", LegacySQLTypeName.STRING)),
            List.of(
                Map.of("datarepo_row_id", drRowId1, "input_value", drRowId1),
                Map.of("datarepo_row_id", drRowId2, "input_value", drRowId2)));

        mockNumRowIds(snapshot, TABLE_2_NAME, 0);

        SnapshotRequestContentsModel requestModel = new SnapshotRequestContentsModel()
            .rowIdSpec(new SnapshotRequestRowIdModel()
                .addTablesItem(new SnapshotRequestRowIdTableModel()
                    .addRowIdsItem(drRowId1).addRowIdsItem(drRowId2)
                    .addColumnsItem(TABLE_2_COL1_NAME)
                    .addColumnsItem(TABLE_2_COL2_NAME)
                    .tableName(TABLE_2_NAME)));
        assertThrows(
            PdaoException.class,
            () -> dao.createSnapshotWithProvidedIds(snapshot, requestModel),
            "Invalid row ids supplied");

        // Verify that the rowIds are properly copied (make sure that it still actually happens)
        verify(bigQueryProjectSnapshot, times(1)).query(
            "INSERT INTO `" + SNAPSHOT_PROJECT_ID + "." + SNAPSHOT_NAME + ".datarepo_row_ids` " +
                "(datarepo_table_id,datarepo_row_id) " +
                "SELECT '" + TABLE_2_ID + "' AS datarepo_table_id, T.row_id AS datarepo_row_id FROM (" +
                "SELECT row_id FROM UNNEST(['" + drRowId1 + "','" + drRowId2 + "']) AS row_id" +
                ") AS T");
    }

    private Snapshot mockSnapshot() {
        DatasetTable tbl1 = DatasetFixtures.generateDatasetTable(
            TABLE_1_NAME,
            TableDataType.STRING,
            List.of(TABLE_1_COL1_NAME, TABLE_1_COL2_NAME))
            .id(TABLE_1_ID);
        tbl1.getColumns().get(0).id(TABLE_1_COL1_ID);
        tbl1.getColumns().get(1).id(TABLE_1_COL2_ID);

        DatasetTable tbl2 = DatasetFixtures.generateDatasetTable(
            TABLE_2_NAME,
            TableDataType.STRING,
            List.of(TABLE_2_COL1_NAME, TABLE_2_COL2_NAME, TABLE_2_COL3_NAME))
            .id(TABLE_2_ID);
        tbl2.getColumns().get(0).id(TABLE_2_COL1_ID);
        tbl2.getColumns().get(1).id(TABLE_2_COL2_ID);
        tbl2.getColumns().get(2).id(TABLE_2_COL3_ID);

        SnapshotTable snpTbl1 = new SnapshotTable()
            .id(SNAPSHOT_TABLE_1_ID)
            .name(tbl1.getName())
            .columns(tbl1.getColumns());
        snpTbl1.getColumns().get(0).id(SNAPSHOT_TABLE_1_COL1_ID);
        snpTbl1.getColumns().get(1).id(SNAPSHOT_TABLE_1_COL2_ID);

        SnapshotTable snpTbl2 = new SnapshotTable()
            .id(SNAPSHOT_TABLE_2_ID)
            .name(tbl2.getName())
            .columns(tbl2.getColumns());
        snpTbl2.getColumns().get(0).id(SNAPSHOT_TABLE_2_COL1_ID);
        snpTbl2.getColumns().get(1).id(SNAPSHOT_TABLE_2_COL2_ID);
        snpTbl2.getColumns().get(2).id(SNAPSHOT_TABLE_2_COL3_ID);

        return new Snapshot()
            .id(SNAPSHOT_ID)
            .name(SNAPSHOT_NAME)
            .description(SNAPSHOT_DESCRIPTION)
            .createdDate(SNAPSHOT_CREATION)
            .profileId(PROFILE_1_ID)
            .projectResource(new GoogleProjectResource()
                .profileId(PROFILE_1_ID)
                .googleProjectId(SNAPSHOT_PROJECT_ID))
            .snapshotTables(List.of(
                snpTbl1,
                snpTbl2))
            .snapshotSources(List.of(
                new SnapshotSource()
                    .dataset(new Dataset()
                        .id(DATASET_ID)
                        .name(DATASET_NAME)
                        .projectResource(new GoogleProjectResource()
                            .profileId(PROFILE_1_ID)
                            .googleProjectId(DATASET_PROJECT_ID))
                        .tables(List.of(tbl1, tbl2))
                        .storage(List.of(new StorageResource()
                            .datasetId(DATASET_ID)
                            .cloudResource(GoogleCloudResource.BIGQUERY)
                            .cloudPlatform(CloudPlatform.GCP)
                            .region(GoogleRegion.NORTHAMERICA_NORTHEAST1))))
                    .assetSpecification(new AssetSpecification()
                        .rootTable(new AssetTable()
                            .datasetTable(tbl1))
                        .rootColumn(new AssetColumn()
                            .datasetTable(tbl1)
                            .datasetColumn(tbl1.getColumns().get(0)))
                        .assetRelationships(List.of(
                            new AssetRelationship()
                                .datasetRelationship(new Relationship()
                                    .fromTable(tbl1).fromColumn(tbl1.getColumns().get(0))
                                    .toTable(tbl2).toColumn(tbl2.getColumns().get(0))))))
                    .snapshotMapTables(List.of(
                        new SnapshotMapTable().fromTable(tbl1).toTable(snpTbl1)
                            .snapshotMapColumns(List.of(
                                new SnapshotMapColumn()
                                    .fromColumn(tbl1.getColumns().get(0)).toColumn(snpTbl1.getColumns().get(0)),
                                new SnapshotMapColumn()
                                    .fromColumn(tbl1.getColumns().get(1)).toColumn(snpTbl1.getColumns().get(1)))),
                        new SnapshotMapTable().fromTable(tbl2).toTable(snpTbl2)
                            // Note: purposefully not mapping the third column and swappign the first two\
                            // for special handling
                            .snapshotMapColumns(List.of(
                                new SnapshotMapColumn()
                                    .fromColumn(tbl2.getColumns().get(0)).toColumn(snpTbl2.getColumns().get(1)),
                                new SnapshotMapColumn()
                                    .fromColumn(tbl2.getColumns().get(1)).toColumn(snpTbl2.getColumns().get(0))))))));
    }

    private void mockNumRowIds(Snapshot snapshot, String tableName, int numRowIds) {
        String datasetProjectId =
            snapshot.getFirstSnapshotSource().getDataset().getProjectResource().getGoogleProjectId();
        String datasetName =
            snapshot.getFirstSnapshotSource().getDataset().getName();
        String snapshotProjectId =
            snapshot.getProjectResource().getGoogleProjectId();
        String snapshotName = snapshot.getName();
        BQTestUtils.mockBQQuery(
            bigQueryProjectSnapshot,
            "SELECT COUNT(1) " +
                "FROM `" + datasetProjectId + ".datarepo_" + datasetName + "." + tableName + "` AS T, " +
                "`" + snapshotProjectId + "." + snapshotName + ".datarepo_row_ids` AS R " +
                "WHERE R.datarepo_row_id = T.datarepo_row_id",
            Schema.of(Field.of("val", LegacySQLTypeName.NUMERIC)),
            List.of(Map.of("val", Integer.toString(numRowIds))));
    }

}
