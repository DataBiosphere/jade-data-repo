package bio.terra.service.tabulardata.google;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.PdaoConstant;
import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.Table;
import bio.terra.common.exception.PdaoException;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.BigQueryPartitionConfigV1;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.dataset.exception.IngestFileNotFoundException;
import bio.terra.service.filedata.google.bq.BigQueryConfiguration;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.exception.BadExternalFileException;
import bio.terra.service.tabulardata.exception.MismatchedRowIdException;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TEMP_TABLE;

@Component
@Profile("google")
public class BigQueryPdao {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryPdao.class);

    private final String datarepoDnsName;
    private final BigQueryConfiguration bigQueryConfiguration;
    private final BigQueryProjectProvider bigQueryProjectProvider;

    @Autowired
    public BigQueryPdao(ApplicationConfiguration applicationConfiguration,
                        BigQueryConfiguration bigQueryConfiguration,
                        BigQueryProjectProvider bigQueryProjectProvider) {
        this.datarepoDnsName = applicationConfiguration.getDnsName();
        this.bigQueryConfiguration = bigQueryConfiguration;
        this.bigQueryProjectProvider = bigQueryProjectProvider;
    }

    @VisibleForTesting
    BigQueryProject bigQueryProjectForDataset(Dataset dataset) {
        return bigQueryProjectProvider.apply(dataset.getProjectResource().getGoogleProjectId());
    }

    @VisibleForTesting
    BigQueryProject bigQueryProjectForSnapshot(Snapshot snapshot) {
        return bigQueryProjectProvider.apply(snapshot.getProjectResource().getGoogleProjectId());
    }

    public void createDataset(Dataset dataset) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        // Keep the dataset name from colliding with a snapshot name by prefixing it.
        // TODO: validate against people using the prefix for snapshots
        String datasetName = prefixName(dataset.getName());
        try {
            // For idempotency, if we find the dataset exists, we assume that we started to
            // create it before and failed in the middle. We delete it and re-create it from scratch.
            if (bigQueryProject.datasetExists(datasetName)) {
                bigQueryProject.deleteDataset(datasetName);
            }

            GoogleRegion region =
                (GoogleRegion) dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BIGQUERY);

            bigQueryProject.createDataset(datasetName, dataset.getDescription(), region);
            bigQueryProject.createTable(
                datasetName, PDAO_LOAD_HISTORY_TABLE, buildLoadDatasetSchema());
            for (DatasetTable table : dataset.getTables()) {
                bigQueryProject.createTable(
                    datasetName, table.getRawTableName(), buildSchema(table, true), table.getBigQueryPartitionConfig());
                bigQueryProject.createTable(
                    datasetName, table.getSoftDeleteTableName(), buildSoftDeletesSchema());
                bigQuery.create(buildLiveView(bigQueryProject.getProjectId(), datasetName, table));
            }
            // TODO: don't catch generic exceptions
        } catch (Exception ex) {
            throw new PdaoException("create dataset failed for " + datasetName, ex);
        }
    }

    private static final String liveViewTemplate =
        "SELECT <columns:{c|R.<c>}; separator=\",\"> FROM `<project>.<dataset>.<rawTable>` R " +
            "LEFT OUTER JOIN `<project>.<dataset>.<sdTable>` S USING (" + PDAO_ROW_ID_COLUMN + ") " +
            "WHERE S." + PDAO_ROW_ID_COLUMN + " IS NULL";

    private TableInfo buildLiveView(String bigQueryProject, String datasetName, DatasetTable table) {
        ST liveViewSql = new ST(liveViewTemplate);
        liveViewSql.add("project", bigQueryProject);
        liveViewSql.add("dataset", datasetName);
        liveViewSql.add("rawTable", table.getRawTableName());
        liveViewSql.add("sdTable", table.getSoftDeleteTableName());

        liveViewSql.add("columns", PDAO_ROW_ID_COLUMN);
        liveViewSql.add("columns", table.getColumns().stream().map(Column::getName).collect(Collectors.toList()));
        if (table.getBigQueryPartitionConfig().getMode() == BigQueryPartitionConfigV1.Mode.INGEST_DATE) {
            liveViewSql.add("columns", "_PARTITIONDATE AS " + PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS);
        }

        TableId liveViewId = TableId.of(datasetName, table.getName());
        return TableInfo.of(liveViewId, ViewDefinition.of(liveViewSql.render()));
    }

    public void createStagingLoadHistoryTable(Dataset dataset, String tableName_FlightId) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        try {
            String datasetName = prefixName(dataset.getName());

            if (bigQueryProject.tableExists(datasetName, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId)) {
                bigQueryProject.deleteTable(datasetName, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId);
            }

            bigQueryProject.createTable(
                datasetName, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId, buildLoadDatasetSchema());
        } catch (Exception ex) {
            throw new PdaoException("create staging load history table failed for " + dataset.getName(), ex);
        }
    }

    public void deleteStagingLoadHistoryTable(Dataset dataset, String flightId) {
        try {
            deleteDatasetTable(dataset, PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + flightId);
        } catch (Exception ex) {
            throw new PdaoException("create staging load history table failed for " + dataset.getName(), ex);
        }
    }

    private Schema buildLoadDatasetSchema() {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(Field.newBuilder("load_tag", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
        fieldList.add(Field.newBuilder("load_time",  LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.REQUIRED)
            .build());
        fieldList.add(Field.newBuilder("source_name", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
        fieldList.add(Field.newBuilder("target_path", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
        fieldList.add(Field.newBuilder("state", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build());
        fieldList.add(Field.newBuilder("file_id", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
        fieldList.add(Field.newBuilder("checksum_crc32c", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
        fieldList.add(Field.newBuilder("checksum_md5", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());
        fieldList.add(Field.newBuilder("error", LegacySQLTypeName.STRING)
            .setMode(Field.Mode.NULLABLE)
            .build());

        return Schema.of(fieldList);
    }

    public static final String insertLoadHistoryToStagingTableTemplate =
        "INSERT INTO `<project>.<dataset>.<stagingTable>`" +
            " (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c, checksum_md5, error)" +
            " VALUES <load_history_array:{v|('<load_tag>', '<load_time>', '<v.sourcePath>', '<v.targetPath>'," +
            " '<v.state>', '<v.fileId>', '<v.checksumCRC>', '<v.checksumMD5>', \"\"\"<v.error>\"\"\")};" +
            " separator=\",\">";

    public void loadHistoryToStagingTable(
        Dataset dataset,
        String tableName_FlightId,
        String loadTag,
        Instant loadTime,
        List<BulkLoadHistoryModel> loadHistoryArray) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        ST sqlTemplate = new ST(insertLoadHistoryToStagingTableTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + tableName_FlightId);

        sqlTemplate.add("load_history_array", loadHistoryArray);
        sqlTemplate.add("load_tag", loadTag);
        sqlTemplate.add("load_time", loadTime);

        bigQueryProject.query(sqlTemplate.render());
    }

    private static final String mergeLoadHistoryStagingTableTemplate =
        "MERGE `<project>.<dataset>.<loadTable>` L" +
            " USING `<project>.<dataset>.<stagingTable>` S" +
            " ON S.load_tag = L.load_tag AND S.load_time = L.load_time" +
                " AND S.file_id = L.file_id" +
            " WHEN NOT MATCHED THEN" +
                " INSERT (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c," +
                    " checksum_md5, error)" +
                " VALUES (load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c," +
                    " checksum_md5, error)";

    public void mergeStagingLoadHistoryTable(
        Dataset dataset,
        String flightId) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        String datasetName = prefixName(dataset.getName());

        // Make sure load_history table exists in dataset, if not - add table
        if (!tableExists(dataset, PDAO_LOAD_HISTORY_TABLE)) {
            bigQueryProject.createTable(
                    datasetName, PDAO_LOAD_HISTORY_TABLE, buildLoadDatasetSchema());
        }

        ST sqlTemplate = new ST(mergeLoadHistoryStagingTableTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", datasetName);
        sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + flightId);
        sqlTemplate.add("loadTable", PDAO_LOAD_HISTORY_TABLE);

        bigQueryProject.query(sqlTemplate.render());
    }

    public boolean deleteDataset(Dataset dataset) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        return bigQueryProject.deleteDataset(prefixName(dataset.getName()));
    }

    // NOTE: The CAST here should be valid for all column types but ARRAYs.
    // We validate that asset root columns are non-arrays as part of dataset creation.
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
    private static final String mapValuesToRowsTemplate =
        "SELECT T." + PDAO_ROW_ID_COLUMN + ", V.input_value FROM (" +
            "SELECT input_value FROM UNNEST([<inputVals:{v|'<v>'}; separator=\",\">]) AS input_value) AS V " +
            "LEFT JOIN `<datasetProject>.<dataset>.<table>` AS T ON V.input_value = CAST(T.<column> AS STRING)";

    // compute the row ids from the input ids and validate all inputs have matches


    // returns a structure with the matching row ids (suitable for calling create snapshot)
    // and any mismatched input values that don't have corresponding row.
    // NOTE: In the fullness of time, we may not do this and kick the function into the UI.
    // So this code assumes there is one source and one set of input values.
    // The query it builds embeds data values into the query in an array. I think it will
    // support about 25,000 input values. If that is not enough there is another, more
    // complicated alternative:
    // - create a scratch table at snapshot creation time
    // - truncate before we start
    // - load the values in
    // - do the query
    // - truncate (even tidier...)
    // So if we need to make this work in the long term, we can take that approach.
    public RowIdMatch mapValuesToRows(SnapshotSource source,
                                      List<String> inputValues) throws InterruptedException {
        // One source: grab it and navigate to the relevant parts
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(source.getDataset());
        String datasetProjectId = datasetBigQueryProject.getProjectId();
        AssetSpecification asset = source.getAssetSpecification();
        Column column = asset.getRootColumn().getDatasetColumn();

        ST sqlTemplate = new ST(mapValuesToRowsTemplate);
        sqlTemplate.add("datasetProject", datasetProjectId);
        sqlTemplate.add("dataset", prefixName(source.getDataset().getName()));
        sqlTemplate.add("table", column.getTable().getName());
        sqlTemplate.add("column", column.getName());
        sqlTemplate.add("inputVals", inputValues);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();
        String sql = sqlTemplate.render();
        logger.debug("mapValuesToRows sql: " + sql);
        TableResult result = datasetBigQueryProject.query(sql);
        for (FieldValueList row : result.iterateAll()) {
            // Test getting these by name
            FieldValue rowId = row.get(0);
            FieldValue inputValue = row.get(1);
            if (rowId.isNull()) {
                rowIdMatch.addMismatch(inputValue.getStringValue());
                logger.debug("rowId=<NULL>" + "  inVal=" + inputValue.getStringValue());
            } else {
                rowIdMatch.addMatch(inputValue.getStringValue(), rowId.getStringValue());
                logger.debug("rowId=" + rowId.getStringValue() + "  inVal=" + inputValue.getStringValue());
            }
        }

        return rowIdMatch;
    }

    public void snapshotCreateBQDataset(BigQueryProject bigQueryProject, Snapshot snapshot) {
        String snapshotName = snapshot.getName();
        // Idempotency: delete possibly partial create.
        if (bigQueryProject.datasetExists(snapshotName)) {
            bigQueryProject.deleteDataset(snapshotName);
        }

        // TODO: When we support multiple datasets per snapshot, this will need to be reworked
        GoogleRegion representativeRegion = (GoogleRegion) snapshot.getFirstSnapshotSource()
            .getDataset()
            .getDatasetSummary()
            .getStorageResourceRegion(GoogleCloudResource.BIGQUERY);
        // create snapshot BQ dataset
        bigQueryProject.createDataset(snapshotName, snapshot.getDescription(), representativeRegion);
    }

    public void snapshotViewCreation(
        BigQueryProject datasetBigQueryProject,
        String datasetProjectId,
        String datasetBqDatasetName,
        String snapshotProjectId,
        Snapshot snapshot,
        BigQuery snapshotBigQuery) {
        // create the views
        List<String> bqTableNames = createViews(datasetProjectId,
                                                datasetBqDatasetName,
                                                snapshotProjectId,
                                                snapshot,
                                                snapshotBigQuery);

        // set authorization on views
        String snapshotName = snapshot.getName();
        List<Acl> acls = convertToViewAcls(snapshotProjectId, snapshotName, bqTableNames);
        datasetBigQueryProject.addDatasetAcls(datasetBqDatasetName, acls);
    }

    private static final String loadRootRowIdsTemplate =
        "INSERT INTO `<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` " +
            "(" + PDAO_TABLE_ID_COLUMN + "," + PDAO_ROW_ID_COLUMN + ") " +
            "SELECT '<tableId>' AS " + PDAO_TABLE_ID_COLUMN + ", T.row_id AS " + PDAO_ROW_ID_COLUMN + " FROM (" +
            "SELECT row_id FROM UNNEST([<rowIds:{id|'<id>'}; separator=\",\">]) AS row_id" +
            ") AS T";

    private static final String validateRowIdsForRootTemplate =
        "SELECT COUNT(1) FROM `<datasetProject>.<dataset>.<table>` AS T, " +
            "`<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE + "` AS R " +
            "WHERE R." + PDAO_ROW_ID_COLUMN + " = T." + PDAO_ROW_ID_COLUMN;

    public void createSnapshot(Snapshot snapshot, List<String> rowIds) throws InterruptedException {
        BigQueryProject snapshotBigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String snapshotProjectId = snapshotBigQueryProject.getProjectId();

        // TODO: When we support multiple datasets per snapshot, this will need to be reworked
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(
            snapshot.getFirstSnapshotSource().getDataset());
        String datasetProjectId = datasetBigQueryProject.getProjectId();

        String snapshotName = snapshot.getName();
        BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();

        // create snapshot BQ dataset
        snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

        // create the row id table
        snapshotBigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

        // populate root row ids. Must happen before the relationship walk.
        // NOTE: when we have multiple sources, we can put this into a loop
        SnapshotSource source = snapshot.getFirstSnapshotSource();
        String datasetBqDatasetName = prefixName(source.getDataset().getName());

        AssetSpecification asset = source.getAssetSpecification();
        Table rootTable = asset.getRootTable().getTable();
        String rootTableId = rootTable.getId().toString();

        if (rowIds.size() > 0) {
            ST sqlTemplate = new ST(loadRootRowIdsTemplate);
            sqlTemplate.add("project", snapshotProjectId);
            sqlTemplate.add("snapshot", snapshotName);
            sqlTemplate.add("dataset", datasetBqDatasetName);
            sqlTemplate.add("tableId", rootTableId);
            sqlTemplate.add("rowIds", rowIds);
            snapshotBigQueryProject.query(sqlTemplate.render());
        }

        ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
        sqlTemplate.add("snapshotProject", snapshotProjectId);
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("datasetProject", datasetProjectId);
        sqlTemplate.add("dataset", datasetBqDatasetName);
        sqlTemplate.add("table", rootTable.getName());

        TableResult result = snapshotBigQueryProject.query(sqlTemplate.render());
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        if (countValue.getLongValue() != rowIds.size()) {
            logger.error("Invalid row ids supplied: rowIds=" + rowIds.size() +
                " count=" + countValue.getLongValue());
            for (String rowId : rowIds) {
                logger.error(" rowIdIn: " + rowId);
            }
            throw new PdaoException("Invalid row ids supplied");
        }

        // walk and populate relationship table row ids
        List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(asset);
        walkRelationships(datasetProjectId,
            datasetBqDatasetName,
            snapshotProjectId,
            snapshotName,
            walkRelationships,
            rootTableId,
            snapshotBigQuery);

        snapshotViewCreation(
            datasetBigQueryProject,
            datasetProjectId,
            datasetBqDatasetName,
            snapshotProjectId,
            snapshot,
            snapshotBigQuery);
    }

    private static final String insertAllLiveViewDataTemplate =
        "INSERT INTO `<snapshotProject>.<snapshot>.<dataRepoTable>` " +
            "(<dataRepoTableId>, <dataRepoRowId>) <liveViewTables>";

    private static final String getLiveViewTableTemplate =
        // TODO pull insert out and loop thru rest w UNION ()
            "(SELECT '<tableId>', <dataRepoRowId> FROM `<datasetProject>.<dataset>.<liveView>`)";

    private static final String mergeLiveViewTablesTemplate =
        "<selectStatements; separator=\" UNION ALL \">";

    private static final String validateSnapshotSizeTemplate =
        "SELECT <rowId> FROM `<snapshotProject>.<snapshot>.<dataRepoTable>` LIMIT 1";


    public String createSnapshotTableFromLiveViews(
        BigQueryProject datasetBigQueryProject,
        List<DatasetTable> tables,
        String datasetBqDatasetName) {

        List<String> selectStatements = new ArrayList<>();

        for (DatasetTable table : tables) {

            TableId liveViewId = TableId.of(datasetBqDatasetName, table.getName());
            String liveViewTableName = liveViewId.getTable();

            ST sqlTableTemplate = new ST(getLiveViewTableTemplate);
            sqlTableTemplate.add("tableId", table.getId());
            sqlTableTemplate.add("dataRepoRowId", PDAO_ROW_ID_COLUMN);
            sqlTableTemplate.add("datasetProject", datasetBigQueryProject.getProjectId());
            sqlTableTemplate.add("dataset", datasetBqDatasetName);
            sqlTableTemplate.add("liveView", liveViewTableName);
            selectStatements.add(sqlTableTemplate.render());
        }
        ST sqlMergeTablesTemplate = new ST(mergeLiveViewTablesTemplate);
        sqlMergeTablesTemplate.add("selectStatements", selectStatements);
        return sqlMergeTablesTemplate.render();
    }

    public void createSnapshotWithLiveViews(
        Snapshot snapshot,
        Dataset dataset) throws InterruptedException {

        BigQueryProject snapshotBigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String snapshotProjectId = snapshotBigQueryProject.getProjectId();
        String snapshotName = snapshot.getName();
        BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();

        String datasetBqDatasetName = prefixName(dataset.getName());
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetProjectId = datasetBigQueryProject.getProjectId();

        // create snapshot BQ dataset
        snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

        // create the row id table (row id col and table id col)
        snapshotBigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

        // get source dataset table live views
        List<DatasetTable> tables = dataset.getTables();

        // create a snapshot table based on the live view data row ids
        String liveViewTables = createSnapshotTableFromLiveViews(datasetBigQueryProject, tables, datasetBqDatasetName);


        ST sqlTemplate = new ST(insertAllLiveViewDataTemplate);
        sqlTemplate.add("snapshotProject", snapshotProjectId);
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("dataRepoTable", PDAO_ROW_ID_TABLE);
        sqlTemplate.add("dataRepoTableId", PDAO_TABLE_ID_COLUMN);
        sqlTemplate.add("dataRepoRowId", PDAO_ROW_ID_COLUMN);
        sqlTemplate.add("liveViewTables", liveViewTables);

        snapshotBigQueryProject.query(sqlTemplate.render());

        ST sqlValidateSnapshotTemplate = new ST(validateSnapshotSizeTemplate);
        sqlValidateSnapshotTemplate.add("rowId", PDAO_ROW_ID_COLUMN);
        sqlValidateSnapshotTemplate.add("snapshotProject", snapshotProjectId);
        sqlValidateSnapshotTemplate.add("snapshot", snapshotName);
        sqlValidateSnapshotTemplate.add("dataRepoTable", PDAO_ROW_ID_TABLE);

        TableResult result = snapshotBigQueryProject.query(sqlValidateSnapshotTemplate.render());
        if (result.getTotalRows() <= 0) {
            throw new PdaoException("This snapshot is empty");
        }

        snapshotViewCreation(datasetBigQueryProject,
            datasetProjectId, datasetBqDatasetName, snapshotProjectId, snapshot, snapshotBigQuery);
    }


    public void createSnapshotWithProvidedIds(
        Snapshot snapshot,
        SnapshotRequestContentsModel contentsModel) throws InterruptedException {

        BigQueryProject snapshotBigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String snapshotProjectId = snapshotBigQueryProject.getProjectId();

        // TODO: When we support multiple datasets per snapshot, this will need to be reworked
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(
            snapshot.getFirstSnapshotSource().getDataset());
        String datasetProjectId = datasetBigQueryProject.getProjectId();

        String snapshotName = snapshot.getName();
        BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();
        SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();

        // create snapshot BQ dataset
        snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

        // create the row id table
        snapshotBigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

        // populate root row ids. Must happen before the relationship walk.
        // NOTE: when we have multiple sources, we can put this into a loop
        SnapshotSource source = snapshot.getFirstSnapshotSource();
        String datasetBqDatasetName = prefixName(source.getDataset().getName());

        for (SnapshotRequestRowIdTableModel table : rowIdModel.getTables()) {
            String tableName = table.getTableName();
            Table sourceTable = source
                .reverseTableLookup(tableName)
                .orElseThrow(() -> new CorruptMetadataException("cannot find destination table: " + tableName));

            List<String> rowIds = table.getRowIds();

            if (rowIds.size() > 0) {
                // we break apart the list of rowIds for better scaleability
                List<List<String>> rowIdChunks = ListUtils.partition(rowIds, 10000);
                // partition returns consecutive sublists of a list, each of the same size (final list may be smaller)
                // partitioning a list containing [a, b, c, d, e] with a partition size of 3 yields [[a, b, c], [d, e]]
                // -- an outer list containing two inner lists of three and two elements, all in the original order.

                for (List<String> rowIdChunk : rowIdChunks) { // each loop will load a chunk of rowIds as an INSERT
                    ST sqlTemplate = new ST(loadRootRowIdsTemplate);
                    sqlTemplate.add("project", snapshotProjectId);
                    sqlTemplate.add("snapshot", snapshotName);
                    sqlTemplate.add("dataset", datasetBqDatasetName);
                    sqlTemplate.add("tableId", sourceTable.getId().toString());
                    sqlTemplate.add("rowIds", rowIdChunk);
                    snapshotBigQueryProject.query(sqlTemplate.render());
                }
            }
            ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
            sqlTemplate.add("snapshotProject", snapshotProjectId);
            sqlTemplate.add("snapshot", snapshotName);
            sqlTemplate.add("datasetProject", datasetProjectId);
            sqlTemplate.add("dataset", datasetBqDatasetName);
            sqlTemplate.add("table", sourceTable.getName());

            TableResult result = snapshotBigQueryProject.query(sqlTemplate.render());
            FieldValueList row = result.iterateAll().iterator().next();
            FieldValue countValue = row.get(0);
            if (countValue.getLongValue() != rowIds.size()) {
                logger.error("Invalid row ids supplied: rowIds=" + rowIds.size() +
                    " count=" + countValue.getLongValue());
                for (String rowId : rowIds) {
                    logger.error(" rowIdIn: " + rowId);
                }
                throw new PdaoException("Invalid row ids supplied");
            }
        }

        snapshotViewCreation(datasetBigQueryProject,
            datasetProjectId, datasetBqDatasetName, snapshotProjectId, snapshot, snapshotBigQuery);
    }

    public void grantReadAccessToSnapshot(Snapshot snapshot, Collection<String> policies) throws InterruptedException {
        grantReadAccessWorker(
            bigQueryProjectForSnapshot(snapshot),
            snapshot.getName(),
            policies);
    }

    public void grantReadAccessToDataset(Dataset dataset, Collection<String> policies) throws InterruptedException {
        grantReadAccessWorker(
            bigQueryProjectForDataset(dataset),
            prefixName(dataset.getName()),
            policies);
    }

    private void grantReadAccessWorker(BigQueryProject bigQueryProject,
                                       String name,
                                       Collection<String> policyGroupEmails) {
        List<Acl> policyGroupAcls = policyGroupEmails
            .stream()
            .map(email -> Acl.of(new Acl.Group(email), Acl.Role.READER))
            .collect(Collectors.toList());
        bigQueryProject.addDatasetAcls(name, policyGroupAcls);
    }

    public boolean datasetExists(Dataset dataset) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetName = prefixName(dataset.getName());
        // bigQueryProject.datasetExists checks whether the BigQuery dataset by the provided name exists
        return bigQueryProject.datasetExists(datasetName);
    }

    public boolean tableExists(Dataset dataset, String tableName) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetName = prefixName(dataset.getName());
        return bigQueryProject.tableExists(datasetName, tableName);
    }

    public boolean snapshotExists(Snapshot snapshot) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        return bigQueryProject.datasetExists(snapshot.getName());
    }

    public boolean deleteSnapshot(Snapshot snapshot) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String snapshotProjectId = bigQueryProject.getProjectId();
        List<SnapshotSource> sources = snapshot.getSnapshotSources();
        if (sources.size() > 0) {
            String datasetName = sources.get(0).getDataset().getName();
            String datasetBqDatasetName = prefixName(datasetName);
            deleteViewAcls(datasetBqDatasetName, snapshot, snapshotProjectId);
        } else {
            logger.warn("Snapshot is missing sources: " + snapshot.getName());
        }
        return bigQueryProject.deleteDataset(snapshot.getName());
    }

    private List<Acl> convertToViewAcls(String projectId, String datasetName, List<String> tableNames) {
        return tableNames
            .stream()
            .map(tableName -> TableId.of(projectId, datasetName, tableName))
            .map(tableId -> Acl.of(new Acl.View(tableId)))
            .collect(Collectors.toList());
    }

    // Load data
    public PdaoLoadStatistics loadToStagingTable(Dataset dataset,
                                                 DatasetTable targetTable,
                                                 String stagingTableName,
                                                 IngestRequestModel ingestRequest) throws InterruptedException {

        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        BigQuery bigQuery = bigQueryProject.getBigQuery();
        TableId tableId = TableId.of(prefixName(dataset.getName()), stagingTableName);
        Schema schema = buildSchema(targetTable, true); // Source does not have row_id
        LoadJobConfiguration.Builder loadBuilder = LoadJobConfiguration.builder(tableId, ingestRequest.getPath())
                .setFormatOptions(buildFormatOptions(ingestRequest))
                .setMaxBadRecords(
                    (ingestRequest.getMaxBadRecords() == null) ? Integer.valueOf(0)
                        : ingestRequest.getMaxBadRecords())
                .setIgnoreUnknownValues(
                    (ingestRequest.isIgnoreUnknownValues() == null) ? Boolean.TRUE
                        : ingestRequest.isIgnoreUnknownValues())
                .setSchema(schema) // docs say this is for target, but CLI provides one for the source
                .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE);

        // This seems like a bug in the BigQuery Java interface.
        // The null marker is CSV-only, but it cannot be set in the format,
        // so we have to special-case here. Grumble...
        if (ingestRequest.getFormat() == IngestRequestModel.FormatEnum.CSV) {
            loadBuilder.setNullMarker(
                    (ingestRequest.getCsvNullMarker() == null) ? ""
                        : ingestRequest.getCsvNullMarker());
        }
        LoadJobConfiguration configuration = loadBuilder.build();

        Job loadJob = bigQuery.create(JobInfo.of(configuration));
        Instant loadJobMaxTime = Instant.now().plusSeconds(TimeUnit.MINUTES.toSeconds(20L));
        while (!loadJob.isDone()) {
            logger.info("Waiting for staging table load job " + loadJob.getJobId().getJob() + " to complete");
            TimeUnit.SECONDS.sleep(5L);

            if (loadJobMaxTime.isBefore(Instant.now())) {
                loadJob.cancel();
                throw new PdaoException("Staging table load failed to complete within timeout - canceled");
            }
        }
        loadJob = loadJob.reload();

        BigQueryError loadJobError = loadJob.getStatus().getError();
        if (loadJobError == null) {
            logger.info("Staging table load job " + loadJob.getJobId().getJob() + " succeeded");
        } else {
            logger.info("Staging table load job " + loadJob.getJobId().getJob() + " failed: " + loadJobError);
            if ("notFound".equals(loadJobError.getReason())) {
                throw new IngestFileNotFoundException("Ingest source file not found: " + ingestRequest.getPath());
            }

            List<String> loadErrors = new ArrayList<>();
            List<BigQueryError> bigQueryErrors = loadJob.getStatus().getExecutionErrors();
            for (BigQueryError bigQueryError : bigQueryErrors) {
                loadErrors.add("BigQueryError: reason=" + bigQueryError.getReason() +
                    " message=" + bigQueryError.getMessage());
            }
            throw new IngestFailureException(
                "Ingest failed with " + loadErrors.size() + " errors - see error details",
                loadErrors);
        }

        // Job completed successfully
        JobStatistics.LoadStatistics loadStatistics = loadJob.getStatistics();

        PdaoLoadStatistics pdaoLoadStatistics = new PdaoLoadStatistics()
            .badRecords(loadStatistics.getBadRecords())
            .rowCount(loadStatistics.getOutputRows())
            .startTime(Instant.ofEpochMilli(loadStatistics.getStartTime()))
            .endTime(Instant.ofEpochMilli(loadStatistics.getEndTime()));
        return pdaoLoadStatistics;
    }

    private static final String addRowIdsToStagingTableTemplate =
        "UPDATE `<project>.<dataset>.<stagingTable>` SET " +
            PDAO_ROW_ID_COLUMN + " = GENERATE_UUID() WHERE " +
            PDAO_ROW_ID_COLUMN + " IS NULL";

    public void addRowIdsToStagingTable(Dataset dataset, String stagingTableName) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        ST sqlTemplate = new ST(addRowIdsToStagingTableTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("stagingTable", stagingTableName);

        bigQueryProject.query(sqlTemplate.render());
    }

    private static final String insertIntoDatasetTableTemplate =
        "INSERT INTO `<project>.<dataset>.<targetTable>` (<columns; separator=\",\">) " +
            "SELECT <columns; separator=\",\"> FROM `<project>.<dataset>.<stagingTable>`";

    public void insertIntoDatasetTable(Dataset dataset,
                                     DatasetTable targetTable,
                                     String stagingTableName) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        ST sqlTemplate = new ST(insertIntoDatasetTableTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("targetTable", targetTable.getRawTableName());
        sqlTemplate.add("stagingTable", stagingTableName);
        sqlTemplate.add("columns", PDAO_ROW_ID_COLUMN);
        targetTable.getColumns().forEach(column -> sqlTemplate.add("columns", column.getName()));

        bigQueryProject.query(sqlTemplate.render());
    }

    private FormatOptions buildFormatOptions(IngestRequestModel ingestRequest) {
        FormatOptions options;
        switch (ingestRequest.getFormat()) {
            case CSV:
                CsvOptions csvDefaults = FormatOptions.csv();

                options = CsvOptions.newBuilder()
                    .setFieldDelimiter(
                        ingestRequest.getCsvFieldDelimiter() == null ? csvDefaults.getFieldDelimiter()
                            : ingestRequest.getCsvFieldDelimiter())
                    .setQuote(
                        ingestRequest.getCsvQuote() == null ? csvDefaults.getQuote()
                            : ingestRequest.getCsvQuote())
                    .setSkipLeadingRows(
                        ingestRequest.getCsvSkipLeadingRows() == null ? csvDefaults.getSkipLeadingRows()
                            : ingestRequest.getCsvSkipLeadingRows())
                    .setAllowQuotedNewLines(
                        ingestRequest.isCsvAllowQuotedNewlines() == null ? csvDefaults.allowQuotedNewLines()
                            : ingestRequest.isCsvAllowQuotedNewlines())
                    .build();
                break;

            case JSON:
                options = FormatOptions.json();
                break;

            default:
                throw new PdaoException("Invalid format option: " + ingestRequest.getFormat());
        }
        return options;
    }

    public boolean deleteDatasetTable(Dataset dataset, String tableName) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        return bigQueryProject.deleteTable(prefixName(dataset.getName()), tableName);
    }

    private static final String getRefIdsTemplate =
        "SELECT <refCol> FROM `<project>.<dataset>.<table>`" +
            "<if(array)> CROSS JOIN UNNEST(<refCol>) AS <refCol><endif>";

    public List<String> getRefIds(Dataset dataset, String tableName, Column refColumn) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        ST sqlTemplate = new ST(getRefIdsTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("table", tableName);
        sqlTemplate.add("refCol", refColumn.getName());
        sqlTemplate.add("array", refColumn.isArrayOf());

        TableResult result = bigQueryProject.query(sqlTemplate.render());
        List<String> refIdArray = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            if (!row.get(0).isNull()) {
                String refId = row.get(0).getStringValue();
                refIdArray.add(refId);
            }
        }

        return refIdArray;
    }

    private static final String getSnapshotRefIdsTemplate =
        "SELECT <refCol> FROM `<datasetProject>.<dataset>.<table>` S, " +
            "`<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R " +
            "<if(array)>CROSS JOIN UNNEST(S.<refCol>) AS <refCol> <endif>" +
            "WHERE S." + PDAO_ROW_ID_COLUMN + " = R." + PDAO_ROW_ID_COLUMN + " AND " +
            "R." + PDAO_TABLE_ID_COLUMN + " = '<tableId>'";

    public List<String> getSnapshotRefIds(Dataset dataset,
                                         Snapshot snapshot,
                                         String tableName,
                                         String tableId,
                                         Column refColumn) throws InterruptedException {
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(dataset);
        BigQueryProject snapshotBigQueryProject = bigQueryProjectForSnapshot(snapshot);

        ST sqlTemplate = new ST(getSnapshotRefIdsTemplate);
        sqlTemplate.add("datasetProject", datasetBigQueryProject.getProjectId());
        sqlTemplate.add("snapshotProject", snapshotBigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("snapshot", snapshot.getName());
        sqlTemplate.add("table", tableName);
        sqlTemplate.add("tableId", tableId);
        sqlTemplate.add("refCol", refColumn.getName());
        sqlTemplate.add("array", refColumn.isArrayOf());

        TableResult result = snapshotBigQueryProject.query(sqlTemplate.render());
        List<String> refIdArray = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            if (!row.get(0).isNull()) {
                String refId = row.get(0).getStringValue();
                refIdArray.add(refId);
            }
        }

        return refIdArray;
    }

    public static String prefixName(String name) {
        return PDAO_PREFIX + name;
    }

    private Schema buildSoftDeletesSchema() {
        return Schema.of(Collections.singletonList(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING)));
    }

    private Schema buildSchema(DatasetTable table, boolean addRowIdColumn) {
        List<Field> fieldList = new ArrayList<>();
        List<String> primaryKeys = table.getPrimaryKey()
            .stream()
            .map(Column::getName)
            .collect(Collectors.toList());

        if (addRowIdColumn) {
            fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        }

        for (Column column : table.getColumns()) {
            Field.Mode mode;
            if (primaryKeys.contains(column.getName())) {
                mode = Field.Mode.REQUIRED;
            } else if (column.isArrayOf()) {
                mode = Field.Mode.REPEATED;
            } else {
                mode = Field.Mode.NULLABLE;
            }
            Field fieldSpec = Field.newBuilder(column.getName(), translateType(column.getType()))
                .setMode(mode)
                .build();

            fieldList.add(fieldSpec);
        }

        return Schema.of(fieldList);
    }

    private Schema tempTableSchema() {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        return Schema.of(fieldList);
    }

    private Schema rowIdTableSchema() {
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING));
        fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        return Schema.of(fieldList);
    }

    // Load row ids
    // We load row ids by building a SQL INSERT statement with the row ids as data.
    // This is more expensive than streaming the input, but does not introduce visibility
    // issues with the new data. It has the same number of values limitation as the
    // validate row ids.
    private String loadRowIdsSql(String snapshotName,
                                 String tableId,
                                 List<String> rowIds,
                                 String projectId,
                                 String softDeletesTableName,
                                 String bqDatasetName) {
        if (rowIds.size() == 0) {
            return null;
        }

        /*
        INSERT INTO `projectId.snapshotName.PDAO_ROW_ID_TABLE` (PDAO_TABLE_ID_COLUMN, PDAO_ROW_ID_COLUMN)
            SELECT 'tableId' AS table_id, T.PDAO_TABLE_ID_COLUMN AS row_id
            FROM (
                SELECT rowid
                FROM UNNEST ([row_id1,row_id2,..,row_idn]) AS rowid
                EXCEPT DISTINCT (
                    SELECT PDAO_ROW_ID_COLUMN FROM softDeletesTableName
                )
            ) AS T
        */

        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO `")
            .append(projectId).append('.').append(snapshotName).append('.').append(PDAO_ROW_ID_TABLE)
            .append("` (").append(PDAO_TABLE_ID_COLUMN).append(",").append(PDAO_ROW_ID_COLUMN)
            .append(") SELECT ").append("'").append(tableId).append("' AS ").append(PDAO_TABLE_ID_COLUMN)
            .append(", T.rowid AS ").append(PDAO_ROW_ID_COLUMN)
            .append(" FROM (SELECT rowid FROM UNNEST([");

        // Put all of the rowids into an array that is unnested into a table
        String prefix = "";
        for (String rowId : rowIds) {
            builder.append(prefix).append("'").append(rowId).append("'");
            prefix = ",";
        }

        builder.append("]) AS rowid EXCEPT DISTINCT ( SELECT ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" FROM `")
            .append(projectId).append(".").append(bqDatasetName).append(".").append(softDeletesTableName)
            .append("`)) AS T");
        return builder.toString();
    }

    /**
     * Check that the incoming row ids actually exist in the root table.
     *
     * Even though these are currently generated within the create snapshot flight, they may
     * be exposed externally in the future, so validating seemed like a good idea.
     * At this point, the only thing we have stored into the row id table are the incoming row ids.
     * We make the equi-join of row id table and root table over row id. We should get one root table row
     * for each row id table row. So we validate by comparing the count of the joined rows against the
     * count of incoming row ids. This will catch duplicate and mismatched row ids.
     *
     * @param datasetBqDatasetName
     * @param snapshotName
     * @param rootTableName
     * @param projectId
     */
    private String validateRowIdsForRootSql(String datasetBqDatasetName,
                                          String snapshotName,
                                          String rootTableName,
                                          String projectId) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(projectId).append('.').append(datasetBqDatasetName).append('.').append(rootTableName)
                .append("` AS T, `")
                .append(projectId).append('.').append(snapshotName).append('.').append(PDAO_ROW_ID_TABLE)
                .append("` AS R WHERE R.")
                .append(PDAO_ROW_ID_COLUMN).append(" = T.").append(PDAO_ROW_ID_COLUMN);
        return builder.toString();
    }

    /**
     * Recursive walk of the relationships. Note that we only follow what is connected.
     * If there are relationships in the asset that are not connected to the root, they will
     * simply be ignored. See the related comment in dataset validator.
     *
     * We operate on a pdao-specific list of the asset relationships so that we can
     * bookkeep which ones we have visited. Since we need to walk relationships in both
     * the from->to and to->from direction, we have to avoid re-walking a traversed relationship
     * or we infinite loop. Trust me, I know... :)
     *
     * TODO: REVIEWERS: should this code detect circular references?
     *
     * @param datasetBqDatasetName
     * @param snapshotName
     * @param walkRelationships - list of relationships to consider walking
     * @param startTableId
     */
    private void walkRelationships(String datasetProjectId,
                                   String datasetBqDatasetName,
                                   String snapshotProjectId,
                                   String snapshotName,
                                   List<WalkRelationship> walkRelationships,
                                   String startTableId,
                                   BigQuery snapshotBigQuery) throws InterruptedException {
        for (WalkRelationship relationship : walkRelationships) {
            if (relationship.isVisited()) {
                continue;
            }

            // NOTE: setting the direction tells the WalkRelationship to change its meaning of from and to.
            // When constructed, it is always in the FROM_TO direction.
            if (StringUtils.equals(startTableId, relationship.getFromTableId())) {
                relationship.setDirection(WalkRelationship.WalkDirection.FROM_TO);
            } else if (StringUtils.equals(startTableId, relationship.getToTableId())) {
                relationship.setDirection(WalkRelationship.WalkDirection.TO_FROM);
            } else {
                // This relationship is not connected to the start table
                continue;
            }
            logger.info("The relationship is being set from column {} in table {} to column {} in table {}",
                relationship.getFromColumnName(),
                relationship.getFromTableName(),
                relationship.getToColumnName(),
                relationship.getToTableName()
            );

            relationship.setVisited();
            storeRowIdsForRelatedTable(
                datasetProjectId,
                datasetBqDatasetName,
                snapshotProjectId,
                snapshotName,
                relationship,
                snapshotBigQuery);
            walkRelationships(
                datasetProjectId,
                datasetBqDatasetName,
                snapshotProjectId,
                snapshotName,
                walkRelationships,
                relationship.getToTableId(),
                snapshotBigQuery);
        }
    }

    // insert the rowIds into the snapshot row ids table and then kick off the rest of the relationship walking
    // once we have the row ids in addition to the asset spec, this should look familiar to wAsset
    public void queryForRowIds(AssetSpecification assetSpecification,
                               Snapshot snapshot,
                               String sqlQuery) throws InterruptedException {
        //snapshot
        BigQueryProject snapshotBigQueryProject = bigQueryProjectForSnapshot(snapshot);
        BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();
        String snapshotProjectId = snapshotBigQueryProject.getProjectId();
        String snapshotName = snapshot.getName();

        //dataset
        // TODO: When we support multiple datasets per snapshot, this will need to be reworked
        Dataset dataset = snapshot.getFirstSnapshotSource().getDataset();
        String datasetBqDatasetName = prefixName(dataset.getName());
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetProjectId = datasetBigQueryProject.getProjectId();


        // TODO add additional validation that the col is the root col

        // create snapshot bq dataset
        try {

            // create snapshot BQ dataset
            snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

            // now create a temp table with all the selected row ids based on the query in it
            snapshotBigQueryProject.createTable(snapshotName, PDAO_TEMP_TABLE, tempTableSchema());

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlQuery)
                .setDestinationTable(TableId.of(snapshotName, PDAO_TEMP_TABLE))
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                .build();

            final TableResult query = executeQueryWithRetry(snapshotBigQuery, queryConfig);

            // get results and validate that it got back more than 0 value
            if (query.getTotalRows() < 1) {
                // should this be a different error?
                throw new InvalidQueryException("Query returned 0 results");
            }

            // join on the root table to validate that the dataset's rootTable.rowid is never null
            // and thus matches the PDAO_ROW_ID_COLUMN
            AssetTable rootAssetTable = assetSpecification.getRootTable();
            Table rootTable = rootAssetTable.getTable();
            String datasetTableName = rootTable.getName();
            String rootTableId = rootTable.getId().toString();

            ST sqlTemplate = new ST(joinTablesToTestForMissingRowIds);
            sqlTemplate.add("snapshotProject", snapshotProjectId);
            sqlTemplate.add("snapshotDatasetName", snapshotName);
            sqlTemplate.add("tempTable", PDAO_TEMP_TABLE);
            sqlTemplate.add("datasetProject", datasetProjectId);
            sqlTemplate.add("datasetDatasetName", datasetBqDatasetName);
            sqlTemplate.add("datasetTable", datasetTableName);
            sqlTemplate.add("commonColumn", PDAO_ROW_ID_COLUMN);

            TableResult result = snapshotBigQueryProject.query(sqlTemplate.render());
            FieldValueList mismatchedCount = result.getValues().iterator().next();
            Long mismatchedCountLong = mismatchedCount.get(0).getLongValue();
            if (mismatchedCountLong > 0) {
                throw new MismatchedValueException("Query results did not match dataset root row ids");
            }

            // TODO should this be pulled up to the top of queryForRowIds() / added to snapshotCreateBQDataset() helper
            snapshotBigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop


            // insert into the PDAO_ROW_ID_TABLE the literal that is the table id
            // and then all the row ids from the temp table
            ST sqlLoadTemplate = new ST(loadRootRowIdsFromTempTableTemplate);
            sqlLoadTemplate.add("snapshotProject", snapshotProjectId);
            sqlLoadTemplate.add("snapshot", snapshotName);
            sqlLoadTemplate.add("dataset", datasetBqDatasetName);
            sqlLoadTemplate.add("tableId", rootTableId);
            sqlLoadTemplate.add("commonColumn", PDAO_ROW_ID_COLUMN); // this is the disc from classic asset
            sqlLoadTemplate.add("tempTable", PDAO_TEMP_TABLE);
            snapshotBigQueryProject.query(sqlLoadTemplate.render());

            //ST sqlValidateTemplate = new ST(validateRowIdsForRootTemplate);
            // TODO do we want to reuse this validation? if yes, maybe mismatchedCount / query should be updated

            // walk and populate relationship table row ids
            List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(assetSpecification);
            walkRelationships(datasetProjectId,
                datasetBqDatasetName,
                snapshotProjectId,
                snapshotName,
                walkRelationships,
                rootTableId,
                snapshotBigQuery);

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop
            snapshotViewCreation(
                datasetBigQueryProject,
                datasetProjectId,
                datasetBqDatasetName,
                snapshotProjectId,
                snapshot,
                snapshotBigQuery);

        } catch (PdaoException ex) {
            // TODO What if the select list doesn't match the temp table schema?
            // TODO what if the query is invalid? Seems like there might be more to catch here.
            throw new PdaoException("createSnapshot failed", ex);
        }
    }


    // NOTE: this will have to be re-written when we support relationships that include
    // more than one column.
    private static final String storeRowIdsForRelatedTableTemplate =
        "WITH merged_table AS (SELECT DISTINCT '<toTableId>' AS " + PDAO_TABLE_ID_COLUMN + ", " +
            "T." + PDAO_ROW_ID_COLUMN + " FROM `<datasetProject>.<dataset>.<toTableName>` T, " +
            "`<datasetProject>.<dataset>.<fromTableName>` F, `<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE +
            "` R " + "WHERE R." + PDAO_TABLE_ID_COLUMN + " = '<fromTableId>' AND " +
            "R." + PDAO_ROW_ID_COLUMN + " = F." + PDAO_ROW_ID_COLUMN + " AND <joinClause>) " +
            "SELECT " + PDAO_TABLE_ID_COLUMN + "," + PDAO_ROW_ID_COLUMN + " FROM merged_table WHERE " +
            PDAO_ROW_ID_COLUMN + " NOT IN " +
            "(SELECT " + PDAO_ROW_ID_COLUMN + " FROM `<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE + "`)";

    private static final String matchNonArrayTemplate =
        "T.<toColumn> = F.<fromColumn>";

    private static final String matchFromArrayTemplate =
        "EXISTS (SELECT 1 FROM UNNEST(F.<fromColumn>) AS flat_from " +
            "WHERE flat_from = T.<toColumn>)";

    private static final String matchToArrayTemplate =
        "EXISTS (SELECT 1 FROM UNNEST(T.<toColumn>) AS flat_to " +
            "WHERE flat_to = F.<fromColumn>)";

    private static final String matchCrossArraysTemplate =
        "EXISTS (SELECT 1 FROM UNNEST(F.<fromColumn>) AS flat_from " +
            "JOIN UNNEST(T.<toColumn>) AS flat_to ON flat_from = flat_to)";

    private static final String joinTablesToTestForMissingRowIds =
        "SELECT COUNT(*) FROM `<snapshotProject>.<snapshotDatasetName>.<tempTable>` AS T " +
            "LEFT JOIN `<datasetProject>.<datasetDatasetName>.<datasetTable>` AS D USING ( <commonColumn> ) " +
            "WHERE D.<commonColumn> IS NULL";

    private static final String loadRootRowIdsFromTempTableTemplate =
        "INSERT INTO `<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE + "` " +
            "(" + PDAO_TABLE_ID_COLUMN + "," + PDAO_ROW_ID_COLUMN + ") " +
            "SELECT '<tableId>' AS " + PDAO_TABLE_ID_COLUMN + ", T.row_id AS " + PDAO_ROW_ID_COLUMN + " FROM (" +
            "SELECT <commonColumn> AS row_id FROM `<snapshotProject>.<snapshot>.<tempTable>` " +
            ") AS T";

    /**
     * Given a relationship, join from the start table to the target table.
     * This may be walking the relationship from the from table to the to table,
     * or walking the relationship from the to table to the from table.
     *
     * @param datasetBqDatasetName - name of the dataset BigQuery dataset
     * @param snapshotName - name of the new snapshot's BigQuery dataset
     * @param relationship - relationship we are walking with its direction set. The class returns
     *                       the appropriate from and to based on that direction.
     * @param datasetProjectId - the project id that this bigquery dataset exists in
     * @param snapshotProjectId - the project id that this bigquery dataset exists in
     * @param bigQuery - a BigQuery instance
     */
    private void storeRowIdsForRelatedTable(String datasetProjectId,
                                            String datasetBqDatasetName,
                                            String snapshotProjectId,
                                            String snapshotName,
                                            WalkRelationship relationship,
                                            BigQuery bigQuery) throws InterruptedException {

        ST joinClauseTemplate;
        if (relationship.getFromColumnIsArray() && relationship.getToColumnIsArray()) {
            joinClauseTemplate = new ST(matchCrossArraysTemplate);
        } else if (relationship.getFromColumnIsArray()) {
            joinClauseTemplate = new ST(matchFromArrayTemplate);
        } else if (relationship.getToColumnIsArray()) {
            joinClauseTemplate = new ST(matchToArrayTemplate);
        } else {
            joinClauseTemplate = new ST(matchNonArrayTemplate);
        }
        joinClauseTemplate.add("fromColumn", relationship.getFromColumnName());
        joinClauseTemplate.add("toColumn", relationship.getToColumnName());

        ST sqlTemplate = new ST(storeRowIdsForRelatedTableTemplate);
        sqlTemplate.add("snapshotProject", snapshotProjectId);
        sqlTemplate.add("datasetProject", datasetProjectId);
        sqlTemplate.add("dataset", datasetBqDatasetName);
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("fromTableId", relationship.getFromTableId());
        sqlTemplate.add("fromTableName", relationship.getFromTableName());
        sqlTemplate.add("toTableId", relationship.getToTableId());
        sqlTemplate.add("toTableName", relationship.getToTableName());
        sqlTemplate.add("joinClause", joinClauseTemplate.render());

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render())
            .setDestinationTable(TableId.of(snapshotName, PDAO_ROW_ID_TABLE))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

        executeQueryWithRetry(bigQuery, queryConfig);
    }

    private static final String createViewsTemplate =
        "SELECT " + PDAO_ROW_ID_COLUMN + ", <columns; separator=\",\"> FROM (" +
            "SELECT S." + PDAO_ROW_ID_COLUMN + ", <mappedColumns; separator=\",\"> " +
            "FROM `<datasetProject>.<dataset>.<mapTable>` S, " +
            "`<snapshotProject>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R WHERE " +
            "S." + PDAO_ROW_ID_COLUMN + " = R." + PDAO_ROW_ID_COLUMN + " AND " +
            "R." + PDAO_TABLE_ID_COLUMN + " = '<tableId>')";

    private List<String> createViews(
        String datasetProjectId,
        String datasetBqDatasetName,
        String snapshotProjectId,
        Snapshot snapshot,
        BigQuery snapshotBigQuery) {
        return snapshot.getTables().stream().map(table -> {
            // Build the FROM clause from the source
            // NOTE: we can put this in a loop when we do multiple sources
            SnapshotSource source = snapshot.getFirstSnapshotSource();
            String snapshotName = snapshot.getName();

            // Find the table map for the table. If there is none, we skip it.
            // NOTE: for now, we know that there will be one, because we generate it directly.
            // In the future when we have more than one, we can just return.
            SnapshotMapTable mapTable = lookupMapTable(table, source);
            if (mapTable == null) {
                throw new PdaoException("No matching map table for snapshot table " + table.getName());
            }
            String snapshotId = snapshot.getId().toString();

            ST sqlTemplate = new ST(createViewsTemplate);
            sqlTemplate.add("datasetProject", datasetProjectId);
            sqlTemplate.add("snapshotProject", snapshotProjectId);
            sqlTemplate.add("dataset", datasetBqDatasetName);
            sqlTemplate.add("snapshot", snapshotName);
            sqlTemplate.add("mapTable", mapTable.getFromTable().getRawTableName());
            sqlTemplate.add("tableId", mapTable.getFromTable().getId().toString());
            table.getColumns().forEach(c -> {
                sqlTemplate.add("columns", c.getName());
                sqlTemplate.add("mappedColumns", sourceSelectSql(snapshotId, c, mapTable));
            });

            // create the view
            String tableName = table.getName();
            String sql = sqlTemplate.render();

            logger.info("Creating view " + snapshotName + "." + tableName + " as " + sql);
            TableId tableId = TableId.of(snapshotName, tableName);
            TableInfo tableInfo = TableInfo.of(tableId, ViewDefinition.of(sql));
            snapshotBigQuery.create(tableInfo);

            return tableName;
        }).collect(Collectors.toList());
    }

    private void deleteViewAcls(
        String datasetBqDatasetName,
        Snapshot snapshot,
        String snapshotProjectId) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        List<String> viewsToDelete = snapshot.getTables().stream().map(table -> {
            // Build the FROM clause from the source
            // NOTE: we can put this in a loop when we do multiple sources
            SnapshotSource source = snapshot.getFirstSnapshotSource();

            // Find the table map for the table. If there is none, we skip it.
            // NOTE: for now, we know that there will be one, because we generate it directly.
            // In the future when we have more than one, we can just return.
            SnapshotMapTable mapTable = lookupMapTable(table, source);
            if (mapTable == null) {
                throw new PdaoException("No matching map table for snapshot table " + table.getName());
            }
            // get the list of table names
            return table.getName();
        }).collect(Collectors.toList());

        // delete the view Acls
        String snapshotName = snapshot.getName();
        viewsToDelete.forEach(tableName -> logger.info("Deleting ACLs for view " + snapshotName + "." + tableName));
        List<Acl> acls = convertToViewAcls(snapshotProjectId, snapshotName, viewsToDelete);
        bigQueryProject.removeDatasetAcls(datasetBqDatasetName, acls);
    }

    /*
     * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
     * any consequences downstream to DRS clients.
     */
    private String sourceSelectSql(String snapshotId, Column targetColumn, SnapshotMapTable mapTable) {
        // In the future, there may not be a column map for a given target column; it might not exist
        // in the table. The logic here covers these cases:
        // 1) no source column: supply NULL
        // 2) source is simple datatype FILEREF or DIRREF:
        //       generate the expression to construct the DRS URI: supply AS target name
        // 3) source is a repeating FILEREF or DIRREF:
        //       generate the unnest/re-nest to construct array of DRS URI: supply AS target name
        // 4) source and target column with same name, just list source name
        // 5) If source and target column with different names: supply AS target name
        String targetColumnName = targetColumn.getName();

        SnapshotMapColumn mapColumn = lookupMapColumn(targetColumn, mapTable);
        if (mapColumn == null) {
            return "NULL AS " + targetColumnName;
        } else {
            TableDataType colType = mapColumn.getFromColumn().getType();
            String mapName = mapColumn.getFromColumn().getName();

            if (colType == TableDataType.FILEREF || colType ==  TableDataType.DIRREF) {

                String drsPrefix = "'drs://" + datarepoDnsName + "/v1_" + snapshotId + "_'";

                if (targetColumn.isArrayOf()) {
                    return "ARRAY(SELECT CONCAT(" + drsPrefix + ", x) " +
                        "FROM UNNEST(" + mapName + ") AS x) AS " + targetColumnName;
                } else {
                    return "CONCAT(" + drsPrefix + ", " + mapName + ") AS " + targetColumnName;
                }
            } else if (StringUtils.equalsIgnoreCase(mapName, targetColumnName)) {
                return targetColumnName;
            } else {
                return mapName + " AS " + targetColumnName;
            }
        }
    }

    // for each table in a dataset (source), collect row id matches ON the row id
    public RowIdMatch matchRowIds(SnapshotSource source, String tableName, List<String> rowIds)
        throws InterruptedException {

        // One source: grab it and navigate to the relevant parts
        BigQueryProject datasetBigQueryProject = bigQueryProjectForDataset(source.getDataset());

        Optional<SnapshotMapTable> optTable = source.getSnapshotMapTables()
            .stream()
            .filter(table -> table.getFromTable().getName().equals(tableName))
            .findFirst();
        // create a column to point to the row id column in the source table to check that passed row ids exist in it
        Column rowIdColumn = new Column()
            .table(optTable.get().getFromTable())
            .name(PDAO_ROW_ID_COLUMN);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();

        List<List<String>> rowIdChunks = ListUtils.partition(rowIds, 10000);
        // partition returns consecutive sublists of a list, each of the same size (final list may be smaller)
        // partitioning a list containing [a, b, c, d, e] with a partition size of 3 yields [[a, b, c], [d, e]]
        // -- an outer list containing two inner lists of three and two elements, all in the original order.

        for (List<String> rowIdChunk : rowIdChunks) { // each loop will load a chunk of rowIds as an INSERT
            // To prevent BQ choking on a huge array, split it up into chunks
            ST sqlTemplate = new ST(mapValuesToRowsTemplate); // This query fails w >100k rows
            sqlTemplate.add("datasetProject", datasetBigQueryProject.getProjectId());
            sqlTemplate.add("dataset", prefixName(source.getDataset().getName()));
            sqlTemplate.add("table", tableName);
            sqlTemplate.add("column", rowIdColumn.getName());
            sqlTemplate.add("inputVals", rowIdChunk);

            String sql = sqlTemplate.render();
            logger.debug("mapValuesToRows sql: " + sql);
            TableResult result = datasetBigQueryProject.query(sql);
            for (FieldValueList row : result.iterateAll()) {
                // Test getting these by name
                FieldValue rowId = row.get(0);
                FieldValue inputValue = row.get(1);
                if (rowId.isNull()) {
                    rowIdMatch.addMismatch(inputValue.getStringValue());
                    logger.debug("rowId=<NULL>" + "  inVal=" + inputValue.getStringValue());
                } else {
                    rowIdMatch.addMatch(inputValue.getStringValue(), rowId.getStringValue());
                    logger.debug("rowId=" + rowId.getStringValue() + "  inVal=" + inputValue.getStringValue());
                }
            }
        }

        return rowIdMatch;
    }

    private SnapshotMapTable lookupMapTable(Table toTable, SnapshotSource source) {
        for (SnapshotMapTable tryMapTable : source.getSnapshotMapTables()) {
            if (tryMapTable.getToTable().getId().equals(toTable.getId())) {
                return tryMapTable;
            }
        }
        return null;
    }

    private SnapshotMapColumn lookupMapColumn(Column toColumn, SnapshotMapTable mapTable) {
        for (SnapshotMapColumn tryMapColumn : mapTable.getSnapshotMapColumns()) {
            if (tryMapColumn.getToColumn().getId().equals(toColumn.getId())) {
                return tryMapColumn;
            }
        }
        return null;
    }

    private LegacySQLTypeName translateType(TableDataType datatype) {
        switch (datatype) {
            case BOOLEAN:   return LegacySQLTypeName.BOOLEAN;
            case BYTES:     return LegacySQLTypeName.BYTES;
            case DATE:      return LegacySQLTypeName.DATE;
            case DATETIME:  return LegacySQLTypeName.DATETIME;
            case DIRREF:    return LegacySQLTypeName.STRING;
            case FILEREF:   return LegacySQLTypeName.STRING;
            case FLOAT:     return LegacySQLTypeName.FLOAT;
            case FLOAT64:   return LegacySQLTypeName.FLOAT;  // match the SQL type
            case INTEGER:   return LegacySQLTypeName.INTEGER;
            case INT64:     return LegacySQLTypeName.INTEGER;  // match the SQL type
            case NUMERIC:   return LegacySQLTypeName.NUMERIC;
            //case RECORD:    return LegacySQLTypeName.RECORD;
            case STRING:    return LegacySQLTypeName.STRING;
            case TEXT:      return LegacySQLTypeName.STRING;   // match the Postgres type
            case TIME:      return LegacySQLTypeName.TIME;
            case TIMESTAMP: return LegacySQLTypeName.TIMESTAMP;
            default: throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
        }
    }

    private String externalTableName(String tableName, String suffix) {
        return String.format("%s%s_%s", PDAO_EXTERNAL_TABLE_PREFIX, tableName, suffix);
    }

    private static final String validateExtTableTemplate =
        "SELECT <rowId> FROM `<project>.<dataset>.<table>` LIMIT 1";

    public void createSoftDeleteExternalTable(Dataset dataset, String path, String tableName, String suffix)
        throws InterruptedException {

        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String extTableName = externalTableName(tableName, suffix);
        TableId tableId = TableId.of(prefixName(dataset.getName()), extTableName);
        Schema schema = Schema.of(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        ExternalTableDefinition tableDef = ExternalTableDefinition.of(path, schema, FormatOptions.csv());
        TableInfo tableInfo = TableInfo.of(tableId, tableDef);
        bigQueryProject.getBigQuery().create(tableInfo);

        // validate that the external table has data
        String sql = new ST(validateExtTableTemplate)
            .add("rowId", PDAO_ROW_ID_COLUMN)
            .add("project", bigQueryProject.getProjectId())
            .add("dataset", tableId.getDataset())
            .add("table", tableId.getTable())
            .render();
        TableResult result = bigQueryProject.query(sql);
        if (result.getTotalRows() == 0L) {
            // either the file at the path is empty or it doesn't exist. error out and let the cleanup begin
            String msg = String.format("No rows found at %s. Likely it is from a bad path or empty file(s).", path);
            throw new BadExternalFileException(msg);
        }
    }

    public boolean deleteSoftDeleteExternalTable(Dataset dataset, String tableName, String suffix)
        throws InterruptedException {

        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String extTableName = externalTableName(tableName, suffix);
        return bigQueryProject.deleteTable(prefixName(dataset.getName()), extTableName);
    }

    private static final String insertSoftDeleteTemplate =
        "INSERT INTO `<project>.<dataset>.<softDeleteTable>` " +
        "SELECT DISTINCT E.<rowId> FROM `<project>.<dataset>.<softDeleteExtTable>` E " +
        "LEFT JOIN `<project>.<dataset>.<softDeleteTable>` S USING (<rowId>) " +
        "WHERE S.<rowId> IS NULL";

    /**
     * Insert row ids into the corresponding soft delete table for each table provided.
     *
     * @param dataset repo dataset that we are deleting data from
     * @param tableNames list of table names that should have corresponding external tables with row ids to soft delete
     * @param suffix a bq-safe version of the flight id to prevent different flights from stepping on each other
     */
    public TableResult applySoftDeletes(Dataset dataset,
                                        List<String> tableNames,
                                        String suffix) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        // we want this soft delete operation to be one parent job with one child-job per query, we we will combine
        // all of the inserts into one statement that we send to bigquery.
        // the soft delete tables have a random suffix on them, we need to fetch those from the db and pass them in
        Map<String, String> softDeleteTableNameLookup = dataset.getTables()
            .stream()
            .collect(Collectors.toMap(DatasetTable::getName, DatasetTable::getSoftDeleteTableName));

        List<String> sqlStatements = tableNames.stream()
            .map(tableName -> new ST(insertSoftDeleteTemplate)
                .add("project", bigQueryProject.getProjectId())
                .add("dataset", prefixName(dataset.getName()))
                .add("softDeleteTable", softDeleteTableNameLookup.get(tableName))
                .add("rowId", PDAO_ROW_ID_COLUMN)
                .add("softDeleteExtTable", externalTableName(tableName, suffix))
                .render())
            .collect(Collectors.toList());

        return bigQueryProject.query(String.join(";", sqlStatements));
    }

    private long getSingleLongValue(TableResult result) {
        FieldValueList fieldValues = result.getValues().iterator().next();
        return fieldValues.get(0).getLongValue();
    }

    /**
     * This join should pair up every rowId in the external table with a corresponding match in the raw table. If there
     * isn't a match in the raw table, then R.rowId will be null and we count that as a mismatch.
     *
     * Note that since this is joining against the raw table, not the the live view, an attempt to soft delete a rowId
     * that has already been soft deleted will not result in a mismatch.
     */
    private static final String validateSoftDeleteTemplate =
        "SELECT COUNT(E.<rowId>) FROM `<project>.<dataset>.<softDeleteExtTable>` E " +
        "LEFT JOIN `<project>.<dataset>.<rawTable>` R USING (<rowId>) " +
        "WHERE R.<rowId> IS NULL";

    /**
     * Goes through each of the provided tables and checks to see if the proposed row ids to soft delete exist in the
     * raw dataset table. This will error out on the first sign of mismatch.
     *
     * @param dataset dataset repo concept object
     * @param tables list of table specs from the DataDeletionRequest
     * @param suffix a string added onto the end of the external table to prevent collisions
     */
    public void validateDeleteRequest(Dataset dataset, List<DataDeletionTableModel> tables, String suffix)
        throws InterruptedException {

        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        for (DataDeletionTableModel table : tables) {
            String tableName = table.getTableName();
            String rawTableName = dataset.getTableByName(tableName).get().getRawTableName();
            String sql = new ST(validateSoftDeleteTemplate)
                .add("rowId", PDAO_ROW_ID_COLUMN)
                .add("project", bigQueryProject.getProjectId())
                .add("dataset", prefixName(dataset.getName()))
                .add("softDeleteExtTable", externalTableName(tableName, suffix))
                .add("rawTable", rawTableName)
                .render();
            TableResult result = bigQueryProject.query(sql);
            long numMismatched = getSingleLongValue(result);

            // shortcut out early, no use wasting more compute
            if (numMismatched > 0) {
                throw new MismatchedRowIdException(
                    String.format("Could not match %s row ids for table %s", numMismatched, tableName));
            }
        }
    }

    /*
     * WARNING: Ensure SQL is validated before executing this method!
     */
    public List<Map<String, Object>> getSnapshotTableData(Snapshot snapshot,
                                                          String sql) throws InterruptedException {
        // execute query and get result
        final BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        final TableResult result = bigQueryProject.query(sql);

        // aggregate into single object
        final FieldList columns = result.getSchema().getFields();
        final List<Map<String, Object>> values = new ArrayList<>();
        result.iterateAll().forEach(rows -> {
            final Map<String, Object> rowData = new HashMap<>();
            columns.forEach(column -> {
                String columnName = column.getName();
                Object fieldValue = rows.get(columnName).getValue();
                rowData.put(columnName, fieldValue);
            });
            values.add(rowData);
        });

        return values;
    }

    // we select from the live view here so that the row counts take into account rows that have been hard deleted
    private static final String rowCountTemplate = "SELECT COUNT(<rowId>) FROM `<project>.<snapshot>.<table>`";

    public Map<String, Long> getSnapshotTableRowCounts(Snapshot snapshot) throws InterruptedException {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        Map<String, Long> rowCounts = new HashMap<>();
        for (SnapshotTable snapshotTable : snapshot.getTables()) {
            String tableName = snapshotTable.getName();
            String sql = new ST(rowCountTemplate)
                .add("rowId", PDAO_ROW_ID_COLUMN)
                .add("project", bigQueryProject.getProjectId())
                .add("snapshot", snapshot.getName())
                .add("table", tableName)
                .render();
            TableResult result = bigQueryProject.query(sql);
            rowCounts.put(tableName, getSingleLongValue(result));
        }
        return rowCounts;
    }

    /**
     * Run query up to a predetermined number of times until it's first success if the failed is a rate limit exception.
     */
    private TableResult executeQueryWithRetry(
        final BigQuery bigQuery,
        final QueryJobConfiguration queryConfig
    ) throws InterruptedException {
        return executeQueryWithRetry(bigQuery, queryConfig, 0);
    }

    /**
     * Run query up to a predetermined number of times until it's first success if the failed is a rate limit exception.
     */
    private TableResult executeQueryWithRetry(
        final BigQuery bigQuery,
        final QueryJobConfiguration queryConfig,
        final int retryNum
    ) throws InterruptedException {
        final int maxRetries = bigQueryConfiguration.getRateLimitRetries();
        final int retryWait = bigQueryConfiguration.getRateLimitRetryWaitMs();

        if (retryNum > 0) {
            logger.info("Retry number {} of a maximum {}", retryNum, maxRetries);
        }
        try {
            return bigQuery.query(queryConfig);
        } catch (final BigQueryException qe) {
            if (
                qe.getError() != null &&
                    Objects.equals(qe.getError().getReason(), "jobRateLimitExceeded") &&
                    retryNum <= maxRetries
            ) {
                logger.warn("Query failed to run due to exceeding the BigQuery update rate limit.  Retrying.");
                try {
                    // Pause before restarting
                    TimeUnit.MILLISECONDS.sleep(retryWait);
                } catch (final InterruptedException ie) {
                    throw new PdaoException("Interrupt exception while waiting to retry", ie);
                }
                return executeQueryWithRetry(bigQuery, queryConfig, retryNum + 1);
            }
            throw new PdaoException("BigQuery query failed the maximum number of times", qe);
        }
    }
}
