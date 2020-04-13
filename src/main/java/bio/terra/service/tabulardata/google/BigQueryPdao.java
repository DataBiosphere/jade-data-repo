package bio.terra.service.tabulardata.google;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.Column;
import bio.terra.common.PdaoConstant;
import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.PrimaryDataAccess;
import bio.terra.common.Table;
import bio.terra.common.exception.PdaoException;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.BigQueryPartitionConfigV1;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.dataset.exception.IngestFileNotFoundException;
import bio.terra.service.dataset.exception.IngestInterruptedException;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDataProject;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.tabulardata.exception.BadExternalFileException;
import bio.terra.service.tabulardata.exception.MismatchedRowIdException;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;

@Component
@Profile("google")
public class BigQueryPdao implements PrimaryDataAccess {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryPdao.class);

    private final String datarepoDnsName;
    private final DataLocationService dataLocationService;

    @Autowired
    public BigQueryPdao(
        ApplicationConfiguration applicationConfiguration,
        DataLocationService dataLocationService) {
        this.datarepoDnsName = applicationConfiguration.getDnsName();
        this.dataLocationService = dataLocationService;
    }

    public BigQueryProject bigQueryProjectForDataset(Dataset dataset) {
        DatasetDataProject projectForDataset = dataLocationService.getOrCreateProject(dataset);
        return BigQueryProject.get(projectForDataset.getGoogleProjectId());
    }

    private BigQueryProject bigQueryProjectForSnapshot(Snapshot snapshot) {
        SnapshotDataProject projectForSnapshot = dataLocationService.getOrCreateProject(snapshot);
        return BigQueryProject.get(projectForSnapshot.getGoogleProjectId());
    }

    @Override
    public void createDataset(Dataset dataset) {
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

            bigQueryProject.createDataset(datasetName, dataset.getDescription());
            for (DatasetTable table : dataset.getTables()) {
                bigQueryProject.createTable(
                    datasetName, table.getRawTableName(), buildSchema(table, true), table.getBigQueryPartitionConfig());
                bigQueryProject.createTable(
                    datasetName, table.getSoftDeleteTableName(), buildSoftDeletesSchema());
                bigQuery.create(buildLiveView(bigQueryProject.getProjectId(), datasetName, table));
            }
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

    @Override
    public boolean deleteDataset(Dataset dataset) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        return bigQueryProject.deleteDataset(prefixName(dataset.getName()));
    }

    private static final String mapValuesToRowsTemplate =
        "SELECT T." + PDAO_ROW_ID_COLUMN + ", V.input_value FROM (" +
            "SELECT input_value FROM UNNEST([<inputVals:{v|'<v>'}; separator=\",\">]) AS input_value) AS V " +
            "LEFT JOIN `<project>.<dataset>.<table>` AS T ON V.input_value = T.<column>";

    // compute the row ids from the input ids and validate all inputs have matches
    // Add new public method that takes the asset and the snapshot source and the input values and
    // returns a structure with the matching row ids (suitable for calling create snapshot)
    // and any mismatched input values that don't have corresponding roww.
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
    @Override
    public RowIdMatch mapValuesToRows(Snapshot snapshot,
                                      SnapshotSource source,
                                      List<String> inputValues) {
        // One source: grab it and navigate to the relevant parts
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        AssetSpecification asset = source.getAssetSpecification();
        Column column = asset.getRootColumn().getDatasetColumn();

        ST sqlTemplate = new ST(mapValuesToRowsTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(source.getDataset().getName()));
        sqlTemplate.add("table", column.getTable().getName());
        sqlTemplate.add("column", column.getName());
        sqlTemplate.add("inputVals", inputValues);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();
        String sql = sqlTemplate.render();
        logger.debug("mapValuesToRows sql: " + sql);
        TableResult result = bigQueryProject.query(sql);
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

    private static final String loadRootRowIdsTemplate =
        "INSERT INTO `<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` " +
            "(" + PDAO_TABLE_ID_COLUMN + "," + PDAO_ROW_ID_COLUMN + ") " +
            "SELECT '<tableId>' AS " + PDAO_TABLE_ID_COLUMN + ", T.row_id AS " + PDAO_ROW_ID_COLUMN + " FROM (" +
            "SELECT row_id FROM UNNEST([<rowIds:{id|'<id>'}; separator=\",\">]) AS row_id" +
            ") AS T";

    private static final String validateRowIdsForRootTemplate =
        "SELECT COUNT(1) FROM `<project>.<dataset>.<table>` AS T, " +
            "`<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` AS R " +
            "WHERE R." + PDAO_ROW_ID_COLUMN + " = T." + PDAO_ROW_ID_COLUMN;

    @Override
    public void createSnapshot(Snapshot snapshot, List<String> rowIds) {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String projectId = bigQueryProject.getProjectId();
        String snapshotName = snapshot.getName();
        BigQuery bigQuery = bigQueryProject.getBigQuery();
        try {
            // Idempotency: delete possibly partial create.
            if (bigQueryProject.datasetExists(snapshotName)) {
                bigQueryProject.deleteDataset(snapshotName);
            }
            bigQueryProject.createDataset(snapshotName, snapshot.getDescription());

            // create the row id table
            bigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop
            SnapshotSource source = snapshot.getSnapshotSources().get(0);
            String datasetBqDatasetName = prefixName(source.getDataset().getName());

            AssetSpecification asset = source.getAssetSpecification();
            Table rootTable = asset.getRootTable().getTable();
            String rootTableId = rootTable.getId().toString();

            if (rowIds.size() > 0) {
                ST sqlTemplate = new ST(loadRootRowIdsTemplate);
                sqlTemplate.add("project", projectId);
                sqlTemplate.add("snapshot", snapshotName);
                sqlTemplate.add("dataset", datasetBqDatasetName);
                sqlTemplate.add("tableId", rootTableId);
                sqlTemplate.add("rowIds", rowIds);
                bigQueryProject.query(sqlTemplate.render());
            }

            ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
            sqlTemplate.add("project", projectId);
            sqlTemplate.add("snapshot", snapshotName);
            sqlTemplate.add("dataset", datasetBqDatasetName);
            sqlTemplate.add("table", rootTable.getName());

            TableResult result = bigQueryProject.query(sqlTemplate.render());
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
            walkRelationships(datasetBqDatasetName, snapshotName, walkRelationships, rootTableId, projectId, bigQuery);

            // create the views
            List<String> bqTableNames = createViews(datasetBqDatasetName, snapshotName, snapshot, projectId, bigQuery);

            // set authorization on views
            List<Acl> acls = convertToViewAcls(projectId, snapshotName, bqTableNames);
            bigQueryProject.addDatasetAcls(datasetBqDatasetName, acls);
        } catch (Exception ex) {
            throw new PdaoException("createSnapshot failed", ex);
        }
    }

    public void createSnapshotWithProvidedIds(
        Snapshot snapshot,
        SnapshotRequestContentsModel contentsModel) {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        String projectId = bigQueryProject.getProjectId();
        String snapshotName = snapshot.getName();
        BigQuery bigQuery = bigQueryProject.getBigQuery();
        SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();


        try {
            // Idempotency: delete possibly partial create.
            if (bigQueryProject.datasetExists(snapshotName)) {
                bigQueryProject.deleteDataset(snapshotName);
            }
            bigQueryProject.createDataset(snapshotName, snapshot.getDescription());

            // create the row id table
            bigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop
            SnapshotSource source = snapshot.getSnapshotSources().get(0);
            String datasetBqDatasetName = prefixName(source.getDataset().getName());


            rowIdModel.getTables().forEach(table -> {
                String tableName = table.getTableName();
                Table sourceTable = source
                    .reverseTableLookup(tableName)
                    .orElseThrow(() -> new CorruptMetadataException("cannot find destination table: " + tableName));

                List<String> rowIds = table.getRowIds();
                if (rowIds.size() > 0) {
                    ST sqlTemplate = new ST(loadRootRowIdsTemplate);
                    sqlTemplate.add("project", projectId);
                    sqlTemplate.add("snapshot", snapshotName);
                    sqlTemplate.add("dataset", datasetBqDatasetName);
                    sqlTemplate.add("tableId", sourceTable.getId().toString());
                    sqlTemplate.add("rowIds", rowIds);
                    bigQueryProject.query(sqlTemplate.render());
                }
                ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
                sqlTemplate.add("project", projectId);
                sqlTemplate.add("snapshot", snapshotName);
                sqlTemplate.add("dataset", datasetBqDatasetName);
                sqlTemplate.add("table", sourceTable.getName());

                TableResult result = bigQueryProject.query(sqlTemplate.render());
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
            });

            // create the views
            List<String> bqTableNames = createViews(datasetBqDatasetName, snapshotName, snapshot, projectId, bigQuery);

            // set authorization on views
            List<Acl> acls = convertToViewAcls(projectId, snapshotName, bqTableNames);
            bigQueryProject.addDatasetAcls(datasetBqDatasetName, acls);
        } catch (Exception ex) {
            throw new PdaoException("createSnapshot failed", ex);
        }
    }

    @Override
    public void addReaderGroupToSnapshot(Snapshot snapshot, String readerPolicyGroupEmail) {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        bigQueryProject.addDatasetAcls(snapshot.getName(),
            Collections.singletonList(Acl.of(new Acl.Group(readerPolicyGroupEmail), Acl.Role.READER)));
    }

    @Override
    public void grantReadAccessToDataset(Dataset dataset, List<String> policyGroupEmails) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        List<Acl> policyGroupAcls = policyGroupEmails
            .stream()
            .map(email -> Acl.of(new Acl.Group(email), Acl.Role.READER))
            .collect(Collectors.toList());
        bigQueryProject.addDatasetAcls(prefixName(dataset.getName()), policyGroupAcls);
    }

    @Override
    public boolean datasetExists(Dataset dataset) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetName = prefixName(dataset.getName());
        // bigQueryProject.datasetExists checks whether the BigQuery dataset by the provided name exists
        return bigQueryProject.datasetExists(datasetName);
    }

    @Override
    public boolean tableExists(Dataset dataset, String tableName) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String datasetName = prefixName(dataset.getName());
        return bigQueryProject.tableExists(datasetName, tableName);
    }

    @Override
    public boolean snapshotExists(Snapshot snapshot) {
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);
        return bigQueryProject.datasetExists(snapshot.getName());
    }

    @Override
    public boolean deleteSnapshot(Snapshot snapshot) {
        return bigQueryProjectForSnapshot(snapshot).deleteDataset(snapshot.getName());
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
                                                 IngestRequestModel ingestRequest) {
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
        try {
            loadJob = loadJob.waitFor();
            if (loadJob.getStatus() == null) {
                throw new PdaoException("Unexpected return from BigQuery job - no getStatus()");
            }
        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            throw new IngestInterruptedException("Ingest was interrupted");
        }

        if (loadJob.getStatus().getError() != null) {
            if ("notFound".equals(loadJob.getStatus().getError().getReason())) {
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

    public void addRowIdsToStagingTable(Dataset dataset, String stagingTableName) {
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
                                     String stagingTableName) {
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

    public boolean deleteDatasetTable(Dataset dataset, String tableName) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        return bigQueryProject.deleteTable(prefixName(dataset.getName()), tableName);
    }

    private static final String getRefIdsTemplate =
        "SELECT <refCol> FROM `<project>.<dataset>.<table>`" +
            "<if(array)> CROSS JOIN UNNEST(<refCol>) AS <refCol><endif>";

    public List<String> getRefIds(Dataset dataset, String tableName, Column refColumn) {

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
        "SELECT <refCol> FROM `<project>.<dataset>.<table>` S, " +
            "`<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R " +
            "<if(array)>CROSS JOIN UNNEST(S.<refCol>) AS <refCol> <endif>" +
            "WHERE S." + PDAO_ROW_ID_COLUMN + " = R." + PDAO_ROW_ID_COLUMN + " AND " +
            "R." + PDAO_TABLE_ID_COLUMN + " = '<tableId>'";

    public List<String> getSnapshotRefIds(Dataset dataset,
                                         String snapshotName,
                                         String tableName,
                                         String tableId,
                                         Column refColumn) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);

        ST sqlTemplate = new ST(getSnapshotRefIdsTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(dataset.getName()));
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("table", tableName);
        sqlTemplate.add("tableId", tableId);
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

    public String prefixName(String name) {
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
    private void walkRelationships(String datasetBqDatasetName,
                                   String snapshotName,
                                   List<WalkRelationship> walkRelationships,
                                   String startTableId,
                                   String projectId,
                                   BigQuery bigQuery) {
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

            relationship.setVisited();
            storeRowIdsForRelatedTable(
                datasetBqDatasetName,
                snapshotName,
                relationship,
                projectId,
                bigQuery);
            walkRelationships(
                datasetBqDatasetName,
                snapshotName,
                walkRelationships,
                relationship.getToTableId(),
                projectId,
                bigQuery);
        }
    }

    // NOTE: this will have to be re-written when we support relationships that include
    // more than one column.
    private static final String storeRowIdsForRelatedTableTemplate =
        "WITH merged_table AS (SELECT DISTINCT '<toTableId>' AS " + PDAO_TABLE_ID_COLUMN + ", " +
            "T." + PDAO_ROW_ID_COLUMN + " FROM `<project>.<dataset>.<toTableName>` T, " +
            "`<project>.<dataset>.<fromTableName>` F, `<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R " +
            "WHERE R." + PDAO_TABLE_ID_COLUMN + " = '<fromTableId>' AND " +
            "R." + PDAO_ROW_ID_COLUMN + " = F." + PDAO_ROW_ID_COLUMN + " AND <joinClause>) " +
            "SELECT " + PDAO_TABLE_ID_COLUMN + "," + PDAO_ROW_ID_COLUMN + " FROM merged_table WHERE " +
            PDAO_ROW_ID_COLUMN + " NOT IN " +
            "(SELECT " + PDAO_ROW_ID_COLUMN + " FROM `<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "`)";

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

    /**
     * Given a relationship, join from the start table to the target table.
     * This may be walking the relationship from the from table to the to table,
     * or walking the relationship from the to table to the from table.
     *
     * @param datasetBqDatasetName - name of the dataset BigQuery dataset
     * @param snapshotName - name of the new snapshot's BigQuery dataset
     * @param relationship - relationship we are walking with its direction set. The class returns
     *                       the appropriate from and to based on that direction.
     * @param projectId - the project id that this bigquery dataset exists in
     * @param bigQuery - a BigQuery instance
     */
    private void storeRowIdsForRelatedTable(String datasetBqDatasetName,
                                            String snapshotName,
                                            WalkRelationship relationship,
                                            String projectId,
                                            BigQuery bigQuery) {

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
        sqlTemplate.add("project", projectId);
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
        try {
            bigQuery.query(queryConfig);
        } catch (InterruptedException ie) {
            throw new PdaoException("Append query unexpectedly interrupted", ie);
        }
    }

    private static final String createViewsTemplate =
        "SELECT " + PDAO_ROW_ID_COLUMN + ", <columns; separator=\",\"> FROM (" +
            "SELECT S." + PDAO_ROW_ID_COLUMN + ", <mappedColumns; separator=\",\"> " +
            "FROM `<project>.<dataset>.<mapTable>` S, " +
            "`<project>.<snapshot>." + PDAO_ROW_ID_TABLE + "` R WHERE " +
            "S." + PDAO_ROW_ID_COLUMN + " = R." + PDAO_ROW_ID_COLUMN + " AND " +
            "R." + PDAO_TABLE_ID_COLUMN + " = '<tableId>')";

    private List<String> createViews(
        String datasetBqDatasetName,
        String snapshotName,
        Snapshot snapshot,
        String projectId,
        BigQuery bigQuery) {
        return snapshot.getTables().stream().map(table -> {
            // Build the FROM clause from the source
            // NOTE: we can put this in a loop when we do multiple sources
            SnapshotSource source = snapshot.getSnapshotSources().get(0);

            // Find the table map for the table. If there is none, we skip it.
            // NOTE: for now, we know that there will be one, because we generate it directly.
            // In the future when we have more than one, we can just return.
            SnapshotMapTable mapTable = lookupMapTable(table, source);
            if (mapTable == null) {
                throw new PdaoException("No matching map table for snapshot table " + table.getName());
            }
            String snapshotId = snapshot.getId().toString();

            ST sqlTemplate = new ST(createViewsTemplate);
            sqlTemplate.add("project", projectId);
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
            bigQuery.create(tableInfo);

            return tableName;
        }).collect(Collectors.toList());
    }

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
            String colType = mapColumn.getFromColumn().getType();
            String mapName = mapColumn.getFromColumn().getName();

            if (StringUtils.equalsIgnoreCase(colType, "FILEREF") ||
                StringUtils.equalsIgnoreCase(colType, "DIRREF")) {

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
    public RowIdMatch matchRowIds(Snapshot snapshot, SnapshotSource source, String tableName, List<String> rowIds) {
        // One source: grab it and navigate to the relevant parts
        BigQueryProject bigQueryProject = bigQueryProjectForSnapshot(snapshot);

        Optional<SnapshotMapTable> optTable = source.getSnapshotMapTables()
            .stream()
            .filter(table -> table.getFromTable().getName().equals(tableName))
            .findFirst();
        // create a column to point to the row id column in the source table to check that passed row ids exist in it
        Column rowIdColumn = new Column()
            .table(optTable.get().getFromTable())
            .name(PDAO_ROW_ID_COLUMN);


        ST sqlTemplate = new ST(mapValuesToRowsTemplate);
        sqlTemplate.add("project", bigQueryProject.getProjectId());
        sqlTemplate.add("dataset", prefixName(source.getDataset().getName()));
        sqlTemplate.add("table", tableName);
        sqlTemplate.add("column", rowIdColumn.getName());
        sqlTemplate.add("inputVals", rowIds);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();
        String sql = sqlTemplate.render();
        logger.debug("mapValuesToRows sql: " + sql);
        TableResult result = bigQueryProject.query(sql);
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

    // TODO: Make an enum for the datatypes in swagger
    private LegacySQLTypeName translateType(String datatype) {
        String uptype = StringUtils.upperCase(datatype);
        switch (uptype) {
            case "BOOLEAN":   return LegacySQLTypeName.BOOLEAN;
            case "BYTES":     return LegacySQLTypeName.BYTES;
            case "DATE":      return LegacySQLTypeName.DATE;
            case "DATETIME":  return LegacySQLTypeName.DATETIME;
            case "DIRREF":    return LegacySQLTypeName.STRING;
            case "FILEREF":   return LegacySQLTypeName.STRING;
            case "FLOAT":     return LegacySQLTypeName.FLOAT;
            case "FLOAT64":   return LegacySQLTypeName.FLOAT;  // match the SQL type
            case "INTEGER":   return LegacySQLTypeName.INTEGER;
            case "INT64":     return LegacySQLTypeName.INTEGER;  // match the SQL type
            case "NUMERIC":   return LegacySQLTypeName.NUMERIC;
            //case "RECORD":    return LegacySQLTypeName.RECORD;
            case "STRING":    return LegacySQLTypeName.STRING;
            case "TEXT":      return LegacySQLTypeName.STRING;   // match the Postgres type
            case "TIME":      return LegacySQLTypeName.TIME;
            case "TIMESTAMP": return LegacySQLTypeName.TIMESTAMP;
            default: throw new IllegalArgumentException("Unknown datatype '" + datatype + "'");
        }
    }

    private String externalTableName(String tableName, String suffix) {
        return String.format("%s%s_%s", PDAO_EXTERNAL_TABLE_PREFIX, tableName, suffix);
    }

    private static final String validateExtTableTemplate =
        "SELECT <rowId> FROM `<project>.<dataset>.<table>` LIMIT 1";

    public void createSoftDeleteExternalTable(Dataset dataset, String path, String tableName, String suffix) {
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

    public boolean deleteSoftDeleteExternalTable(Dataset dataset, String tableName, String suffix) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        String extTableName = externalTableName(tableName, suffix);
        return bigQueryProject.deleteTable(prefixName(dataset.getName()), extTableName);
    }

    private static final String insertSoftDeleteTemplate =
        "INSERT INTO `<project>.<dataset>.<softDeleteTable>` " +
        "SELECT DISTINCT <rowId> FROM `<project>.<dataset>.<softDeleteExtTable>`";

    /**
     * Insert row ids into the corresponding soft delete table for each table provided.
     *
     * @param dataset repo dataset that we are deleting data from
     * @param tableNames list of table names that should have corresponding external tables with row ids to soft delete
     * @param suffix a bq-safe version of the flight id to prevent different flights from stepping on each other
     */
    public TableResult applySoftDeletes(Dataset dataset,
                                 List<String> tableNames,
                                 String suffix) {
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
     * This join should pair up every rowId in the external table with a corresponding match in the live view. If there
     * isn't a match in the live view, then V.rowId will be null and we count that as a mismatch.
     *
     * Note that since this is joining against the live view, an attempt to soft delete a rowId that has already been
     * soft deleted will result in a mismatch.
     */
    private static final String validateSoftDeleteTemplate =
        "SELECT COUNT(E.<rowId>) FROM `<project>.<dataset>.<softDeleteExtTable>` E " +
        "LEFT JOIN `<project>.<dataset>.<liveView>` V USING (<rowId>) " +
        "WHERE V.<rowId> IS NULL";

    /**
     * Goes through each of the provided tables and checks to see if the proposed row ids to soft delete exist in the
     * dataset table. This will error out on the first sign of mismatch.
     *
     * @param dataset dataset repo concept object
     * @param tables list of table specs from the DataDeletionRequest
     * @param suffix a string added onto the end of the external table to prevent collisions
     */
    public void validateDeleteRequest(Dataset dataset, List<DataDeletionTableModel> tables, String suffix) {
        BigQueryProject bigQueryProject = bigQueryProjectForDataset(dataset);
        for (DataDeletionTableModel table : tables) {
            String tableName = table.getTableName();
            String sql = new ST(validateSoftDeleteTemplate)
                .add("rowId", PDAO_ROW_ID_COLUMN)
                .add("project", bigQueryProject.getProjectId())
                .add("dataset", prefixName(dataset.getName()))
                .add("softDeleteExtTable", externalTableName(tableName, suffix))
                .add("liveView", tableName)
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
}
