package bio.terra.pdao.bigquery;

import bio.terra.flight.exception.IngestFailureException;
import bio.terra.flight.exception.IngestInterruptedException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Column;
import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotMapColumn;
import bio.terra.metadata.DataSnapshotMapTable;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.RowIdMatch;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.pdao.exception.PdaoException;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
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
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static bio.terra.pdao.PdaoConstant.PDAO_PREFIX;
import static bio.terra.pdao.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.pdao.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.pdao.PdaoConstant.PDAO_TABLE_ID_COLUMN;

@Component
@Profile("google")
public class BigQueryPdao implements PrimaryDataAccess {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.pdao.bigquery");

    private final BigQuery bigQuery;
    private final String projectId;
    private final String datarepoDnsName;

    @Autowired
    public BigQueryPdao(BigQuery bigQuery, String bigQueryProjectId, String datarepoDnsName) {
        this.bigQuery = bigQuery;
        this.projectId = bigQueryProjectId;
        this.datarepoDnsName = datarepoDnsName;
    }

    @Override
    public boolean datasetExists(String name) {
        return existenceCheck(prefixName(name));
    }

    @Override
    public void createDataset(DrDataset dataset) {
        // Keep the dataset name from colliding with a data snapshot name by prefixing it.
        String datasetName = prefixName(dataset.getName());
        try {
            // For idempotency, if we find the dataset exists, we assume that we started to
            // create it before and failed in the middle. We delete it and re-create it from scratch.
            if (datasetExists(dataset.getName())) {
                deleteDataset(dataset);
            }

            createContainer(datasetName, dataset.getDescription());
            for (Table table : dataset.getTables()) {
                createTable(datasetName, table);
            }
        } catch (Exception ex) {
            throw new PdaoException("create dataset failed for " + datasetName, ex);
        }
    }

    @Override
    public boolean deleteDataset(DrDataset dataset) {
        return deleteContainer(prefixName(dataset.getName()));
    }

    @Override
    public boolean dataSnapshotExists(String dataSnapshotName) {
        return existenceCheck(prefixName(dataSnapshotName));
    }

    // compute the row ids from the input ids and validate all inputs have matches
    // Add new public method that takes the asset and the data snapshot source and the input values and
    // returns a structure with the matching row ids (suitable for calling create data snapshot)
    // and any mismatched input values that don't have corresponding roww.
    // NOTE: In the fullness of time, we may not do this and kick the function into the UI.
    // So this code assumes there is one source and one set of input values.
    // The query it builds embeds data values into the query in an array. I think it will
    // support about 25,000 input values. It that is not enough there is another, more
    // complicated alternative:
    // - create a scratch table at data snapshot creation time
    // - truncate before we start
    // - load the values in
    // - do the query
    // - truncate (even tidier...)
    // So if we need to make this work in the long term, we can take that approach.
    @Override
    public RowIdMatch mapValuesToRows(DataSnapshot dataSnapshot,
                                      DataSnapshotSource source,
                                      List<String> inputValues) {
        /*
            Making this SQL query:
            SELECT T.datarepo_row_id, T.<dataset-column>, V.inputValue
            FROM (select inputValue from unnest(['inputValue0', 'inputValue1', ...]) as inputValue) AS V
            LEFT JOIN <dataset-table> AS T
            ON T.<dataset-column> = V.inputValue
        */

        // One source: grab it and navigate to the relevant parts
        AssetSpecification asset = source.getAssetSpecification();
        Column column = asset.getRootColumn().getDatasetColumn();
        String datasetColumnName = column.getName();
        String datasetTableName = column.getTable().getName();
        String datasetBqDatasetName = prefixName(source.getDataset().getName());

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT T.")
                .append(PDAO_ROW_ID_COLUMN)
                .append(", V.inputValue FROM (SELECT inputValue FROM UNNEST([");

        // Put all of the values into an array that is unnested into a table
        String prefix = "";
        for (String inval : inputValues) {
            builder.append(prefix).append("'").append(inval).append("'");
            prefix = ",";
        }

        builder.append("]) AS inputValue) AS V LEFT JOIN `")
                .append(projectId)
                .append('.')
                .append(datasetBqDatasetName)
                .append('.')
                .append(datasetTableName)
                .append("` AS T ON V.inputValue = T.")
                .append(datasetColumnName);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();
        String sql = builder.toString();
        logger.debug("mapValuesToRows sql: " + sql);
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
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
        } catch (InterruptedException ie) {
            throw new PdaoException("Map values to rows query unexpectedly interrupted", ie);
        }

        return rowIdMatch;
    }

    @Override
    public void createDataSnapshot(DataSnapshot dataSnapshot, List<String> rowIds) {
        String dataSnapshotName = dataSnapshot.getName();
        try {
            // Idempotency: delete possibly partial create.
            if (existenceCheck(dataSnapshotName)) {
                deleteContainer(dataSnapshotName);
            }

            createContainer(dataSnapshotName, dataSnapshot.getDescription());

            // create the row id table
            createRowIdTable(dataSnapshotName);

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop
            DataSnapshotSource source = dataSnapshot.getDataSnapshotSources().get(0);
            String datasetBqDatasetName = prefixName(source.getDataset().getName());

            storeRowIdsForRoot(datasetBqDatasetName, dataSnapshotName, source, rowIds);

            // walk and populate relationship table row ids
            AssetSpecification asset = source.getAssetSpecification();
            String rootTableId = asset.getRootTable().getTable().getId().toString();
            List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(asset);

            walkRelationships(datasetBqDatasetName, dataSnapshotName, walkRelationships, rootTableId);

            // create the views
            List<String> bqTableNames = createViews(datasetBqDatasetName, dataSnapshotName, dataSnapshot);

            // set authorization on views
            authorizeDataSnapshotViewsForDataset(dataSnapshotName, datasetBqDatasetName, bqTableNames);
        } catch (Exception ex) {
            throw new PdaoException("createDataSnapshot failed", ex);
        }
    }

    // for each dataset that is a part of the data snapshot, we will add Acls for the
    // data snapshot tables from that dataset
    @Override
    public void authorizeDataSnapshotViewsForDataset(String dataSnapshotName, String datasetName,
                                                     List<String> tableNames) {
        addAclsToBqDataset(datasetName, convertToViewAcls(dataSnapshotName, tableNames));
    }

    @Override
    public void removeDataSnapshotAuthorizationFromDataset(
        String dataSnapshotName, String datasetName, List<String> tableNames) {
        removeAclsFromBqDataset(datasetName, convertToViewAcls(dataSnapshotName, tableNames));
    }

    private List<Acl> convertToViewAcls(
        String dataSnapshotName, List<String> tableNames) {
        return tableNames.stream().map(tableName ->
                makeViewAcl(dataSnapshotName, tableName)).collect(Collectors.toList());
    }

    private Acl makeViewAcl(String dataSnapshotName, String tableName) {
        TableId tableId = TableId.of(projectId, dataSnapshotName, tableName);
        return Acl.of(new Acl.View(tableId));
    }

    @Override
    public void addReaderGroupToDataSnapshot(String dataSnapshotId, String readersEmail) {
        // add the reader group to the list of Acls on the jade data snapshot
        addAclsToBqDataset(
            dataSnapshotId,
            Collections.singletonList(Acl.of(new Acl.Group(readersEmail), Acl.Role.READER)));
    }

    private void addAclsToBqDataset(String bqDatasetId, List<Acl> acls) {
        Dataset bqDataset = bigQuery.getDataset(bqDatasetId);
        List<Acl> beforeAcls = bqDataset.getAcl();
        ArrayList<Acl> newAcls = new ArrayList<>(beforeAcls);
        newAcls.addAll(acls);
        logger.debug("before als: " + beforeAcls.toString());
        logger.debug("acls after: " + newAcls.toString());
        logger.debug("acls added: " + acls.toString());
        updateBqDataset(bqDataset.toBuilder().setAcl(newAcls).build());
    }

    private void removeAclsFromBqDataset(String bqDatasetId, List<Acl> acls) {
        Dataset bqDataset = bigQuery.getDataset(bqDatasetId);
        if (bqDataset != null) {  // can be null if create data snapshot step failed before it was created
            Set<Acl> datasetAcls = new HashSet(bqDataset.getAcl());
            datasetAcls.removeAll(acls);
            logger.debug("new bq acls: " + datasetAcls.toString());
            logger.debug("acls removed: " + acls.toString());
            updateBqDataset(bqDataset.toBuilder().setAcl(new ArrayList(datasetAcls)).build());
        }
    }

    public void updateBqDataset(DatasetInfo info) {
        bigQuery.update(info);
    }

    @Override
    public boolean deleteDataSnapshot(DataSnapshot dataSnapshot) {
        List<DataSnapshotSource> sources = dataSnapshot.getDataSnapshotSources();
        sources.stream().forEach(dsSource ->
                removeDataSnapshotAuthorizationFromDataset(dataSnapshot.getName(), dsSource.getDataset().getName(),
                dsSource.getAssetSpecification().getAssetTables().stream().map(assetTable ->
                    assetTable.getTable().getName()).collect(Collectors.toList())));
        return deleteContainer(dataSnapshot.getName());
    }

    // Load data
    public PdaoLoadStatistics loadToStagingTable(DrDataset dataset,
                                                 Table targetTable,
                                                 String stagingTableName,
                                                 IngestRequestModel ingestRequest) {

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

    public void addRowIdsToStagingTable(DrDataset dataset, String stagingTableName) {
        /*
         * UPDATE `project.dataset.stagingtable`
         * SET datarepo_row_id = GENERATE_UUID()
         * WHERE datarepo_row_id IS NULL
         */
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ")
            .append("`")
            .append(projectId)
            .append(".")
            .append(prefixName(dataset.getName()))
            .append(".")
            .append(stagingTableName)
            .append("` SET ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" = GENERATE_UUID() WHERE ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" IS NULL");

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql.toString()).build();
            bigQuery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Update staging table unexpectedly interrupted", e);
        }
    }

    public void insertIntoDatasetTable(DrDataset dataset,
                                       Table targetTable,
                                       String stagingTableName) {
        /*
         * INSERT INTO `project.dataset.datasettable`
         * (<column names...>)
         * SELECT <column names...>
         * FROM `project.dataset.datasettable`
         */
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT ")
            .append("`")
            .append(projectId)
            .append(".")
            .append(prefixName(dataset.getName()))
            .append(".")
            .append(targetTable.getName())
            .append("` (");
        buildColumnList(sql, targetTable, true);
        sql.append(") SELECT ");
        buildColumnList(sql, targetTable, true);
        sql.append(" FROM `")
            .append(projectId)
            .append(".")
            .append(prefixName(dataset.getName()))
            .append(".")
            .append(stagingTableName)
            .append("`");

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql.toString()).build();
            bigQuery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Insert staging into dataset table unexpectedly interrupted", e);
        }
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

    public boolean deleteTable(String datasetName, String tableName) {
        try {
            TableId tableId = TableId.of(projectId, prefixName(datasetName), tableName);
            return bigQuery.delete(tableId);
        } catch (Exception ex) {
            throw new PdaoException("Failed attempt delete of dataset: " +
                datasetName + " table: " + tableName, ex);
        }
    }

    public List<String> getRefIds(String datasetName, String tableName, Column refColumn) {
        /*
          For simple columns:
            SELECT refColumnName FROM stagingTable
          For repeating columns this flattens all of the arrays into one result column:
            SELECT x
            FROM stagingTable
            CROSS JOIN UNNEST(refColumnName) AS x
         */
        List<String> refIdArray = new ArrayList<>();

        String datasetBqDatasetName = prefixName(datasetName);
        StringBuilder builder = new StringBuilder();

        if (refColumn.isArrayOf()) {
            builder.append("SELECT x FROM `")
                .append(projectId).append('.').append(datasetBqDatasetName).append('.').append(tableName)
                .append("` CROSS JOIN UNNEST(")
                .append(refColumn.getName())
                .append(") AS x");
        } else {
            builder.append("SELECT ")
                .append(refColumn.getName())
                .append(" FROM `")
                .append(projectId).append('.').append(datasetBqDatasetName).append('.').append(tableName)
                .append("`");
        }
        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
            for (FieldValueList row : result.iterateAll()) {
                if (!row.get(0).isNull()) {
                    String refId = row.get(0).getStringValue();
                    refIdArray.add(refId);
                }
            }

        } catch (InterruptedException ie) {
            throw new PdaoException("Get ref ids query unexpectedly interrupted", ie);
        }

        return refIdArray;
    }

    public List<String> getDataSnapshotRefIds(String datasetName,
                                         String dataSnapshotName,
                                         String tableName,
                                         String tableId,
                                         Column refColumn) {
        /*
          For scalar columns we do this:
            SELECT refColumnName
            FROM <dataset table> S, datarepo_row_ids R
            WHERE S.datarepo_row_id = R.datarepo_row_id
            AND R.datarepo_table_id = '<dataset table id>'

          For array columns we flatten the ref column by adding the cross join:
            SELECT refColumnName
            FROM <dataset table> S, datarepo_row_ids R

            CROSS JOIN UNNEST(S.refColumnName) AS refColumnName

            WHERE S.datarepo_row_id = R.datarepo_row_id
            AND R.datarepo_table_id = '<dataset table id>'
         */
        List<String> refIdArray = new ArrayList<>();

        String datasetBqDataSnapshotName = prefixName(datasetName);
        String refColumnName = refColumn.getName();
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ")
            .append(refColumnName)
            .append(" FROM `")
            .append(projectId).append('.').append(datasetBqDataSnapshotName).append('.').append(tableName)
            .append("` S, `")
            .append(projectId).append('.').append(dataSnapshotName).append('.').append(PDAO_ROW_ID_TABLE)
            .append("` R");

        if (refColumn.isArrayOf()) {
            builder.append(" CROSS JOIN UNNEST(S.")
                .append(refColumnName)
                .append(") ")
                .append(refColumnName);
        }

        builder.append(" WHERE S.")
            // where row_id matches and table_id matches
            .append(PDAO_ROW_ID_COLUMN)
            .append(" = R.")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" AND R.")
            .append(PDAO_TABLE_ID_COLUMN)
            .append(" = '")
            .append(tableId)
            .append("'");

        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
            for (FieldValueList row : result.iterateAll()) {
                if (!row.get(0).isNull()) {
                    String refId = row.get(0).getStringValue();
                    refIdArray.add(refId);
                }
            }

        } catch (InterruptedException ie) {
            throw new PdaoException("Get dataSnapshot ref ids query unexpectedly interrupted", ie);
        }

        return refIdArray;
    }


    private boolean existenceCheck(String name) {
        try {
            DatasetId bqDatasetId = DatasetId.of(projectId, name);
            Dataset bqDataset = bigQuery.getDataset(bqDatasetId);
            return (bqDataset != null);
        } catch (Exception ex) {
            throw new PdaoException("existence check failed for " + name, ex);
        }
    }

    private String prefixName(String name) {
        return PDAO_PREFIX + name;
    }

    private DatasetId createContainer(String name, String description) {
        DatasetInfo bqDatasetInfo = DatasetInfo.newBuilder(name)
            .setDescription(description)
            .build();
        Dataset bqContainer = bigQuery.create(bqDatasetInfo);
        return bqContainer.getDatasetId();
    }

    private boolean deleteContainer(String name) {
        try {
            DatasetId bqDatasetId = DatasetId.of(projectId, name);
            return bigQuery.delete(bqDatasetId, BigQuery.DatasetDeleteOption.deleteContents());
        } catch (Exception ex) {
            throw new PdaoException("delete failed for " + name, ex);
        }
    }

    private void createTable(String containerName, Table table) {
        TableId tableId = TableId.of(containerName, table.getName());
        Schema schema = buildSchema(table, true);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    private Schema buildSchema(Table table, boolean addRowIdColumn) {
        List<Field> fieldList = new ArrayList<>();

        if (addRowIdColumn) {
            fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        }

        for (Column column : table.getColumns()) {
            Field fieldSpec = Field.newBuilder(column.getName(), translateType(column.getType()))
                .setMode(column.isArrayOf() ? Field.Mode.REPEATED : Field.Mode.NULLABLE)
                .build();

            fieldList.add(fieldSpec);
        }

        return Schema.of(fieldList);
    }

    private void createRowIdTable(String dataSnapshotName) {
        TableId tableId = TableId.of(dataSnapshotName, PDAO_ROW_ID_TABLE);
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING));
        fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        Schema schema = Schema.of(fieldList);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    private void storeRowIdsForRoot(String datasetBqDatasetName,
                                    String dataSnapshotName,
                                    DataSnapshotSource source,
                                    List<String> rowIds) {
        AssetSpecification asset = source.getAssetSpecification();
        Table rootTable = asset.getRootTable().getTable();
        loadRootRowIds(dataSnapshotName, rootTable.getId().toString(), rowIds);
        validateRowIdsForRoot(datasetBqDatasetName, dataSnapshotName, rootTable.getName(), rowIds);
    }

    // Load row ids
    // We load row ids by building a SQL INSERT statement with the row ids as data.
    // This is more expensive than streaming the input, but does not introduce visibility
    // issues with the new data. It has the same number of values limitation as the
    // validate row ids.
    private void loadRootRowIds(String dataSnapshotName, String tableId, List<String> rowIds) {
        if (rowIds.size() == 0) {
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO `")
            .append(projectId).append('.').append(dataSnapshotName).append('.').append(PDAO_ROW_ID_TABLE)
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

        builder.append("]) AS rowid) AS T");
        String sql = builder.toString();

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            bigQuery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Insert root row ids unexpectedly interrupted", e);
        } catch (BigQueryException ex) {
            throw new PdaoException("Failure inserting root row ids", ex);
        }
    }

    /**
     * Check that the incoming row ids actually exist in the root table.
     *
     * Even though these are currently generated within the create dataSnapshot flight, they may
     * be exposed externally in the future, so validating seemed like a good idea.
     * At this point, the only thing we have stored into the row id table are the incoming row ids.
     * We make the equi-join of row id table and root table over row id. We should get one root table row
     * for each row id table row. So we validate by comparing the count of the joined rows against the
     * count of incoming row ids. This will catch duplicate and mismatched row ids.
     *
     * @param datasetBqDatasetName
     * @param dataSnapshotName
     * @param rootTableName
     * @param rowIds
     */
    private void validateRowIdsForRoot(String datasetBqDatasetName,
                                       String dataSnapshotName,
                                       String rootTableName,
                                       List<String> rowIds) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(projectId).append('.').append(datasetBqDatasetName).append('.').append(rootTableName)
                .append("` AS T, `")
                .append(projectId).append('.').append(dataSnapshotName).append('.').append(PDAO_ROW_ID_TABLE)
                .append("` AS R WHERE R.")
                .append(PDAO_ROW_ID_COLUMN).append(" = T.").append(PDAO_ROW_ID_COLUMN);
        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
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
        } catch (InterruptedException ie) {
            throw new PdaoException("Validate row ids query unexpectedly interrupted", ie);
        }
    }

    /**
     * Recursive walk of the relationships. Note that we only follow what is connected.
     * If there are relationships in the asset that are not connected to the root, they will
     * simply be ignored. See the related comment in dataset validator.
     *
     * We operate on a pdoa-specific list of the asset relationships so that we can
     * bookkeep which ones we have visited. Since we need to walk relationships in both
     * the from->to and to->from direction, we have to avoid re-walking a traversed relationship
     * or we infinite loop. Trust me, I know... :)
     *
     * TODO: REVIEWERS: should this code detect circular references?
     *
     * @param datasetBqDatasetName
     * @param dataSnapshotName
     * @param walkRelationships - list of relationships to consider walking
     * @param startTableId
     */
    private void walkRelationships(String datasetBqDatasetName,
                                   String dataSnapshotName,
                                   List<WalkRelationship> walkRelationships,
                                   String startTableId) {
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
            storeRowIdsForRelatedTable(datasetBqDatasetName, dataSnapshotName, relationship);
            walkRelationships(datasetBqDatasetName, dataSnapshotName, walkRelationships, relationship.getToTableId());
        }
    }

    /**
     * Given a relationship, join from the start table to the target table.
     * This may be walking the relationship from the from table to the to table,
     * or walking the relationship from the to table to the from table.
     *
     * @param datasetBqDatasetName - name of the dataset BigQuery dataset
     * @param dataSnapshotName - name of the new dataSnapshot's BigQuery dataset
     * @param relationship - relationship we are walking with its direction set. The class returns
     *                       the appropriate from and to based on that direction.
     */
    private void storeRowIdsForRelatedTable(String datasetBqDatasetName,
                                            String dataSnapshotName,
                                            WalkRelationship relationship) {
        // NOTE: this will have to be re-written when we support relationships that include
        // more than one column.
        /*
            The constructed SQL varies depending on if the from/to column is an array.

            Common SQL is:
                SELECT DISTINCT 'toTableId' AS PDAO_TABLE_ID_COLUMN, T.PDAO_ROW_ID_COLUMN
                FROM toTableName T, fromTableName F, ROW_ID_TABLE_NAME R
                WHERE R.PDAO_TABLE_ID_COLUMN = 'fromTableId'
                  AND R.PDAO_ROW_ID_COLUMN = F.PDAO_ROW_ID_COLUMN

            If neither column is an array, add:
                  AND T.toColumnName = F.fromColumnName

            If 'from' is an array, add:
                  AND EXISTS (SELECT 1
                              FROM UNNEST(F.fromColumnName) AS flat_from
                              WHERE flat_from = T.toColumnName)

            If 'to' is an array, add:
                  AND EXISTS (SELECT 1
                              FROM UNNEST(T.toColumnName) AS flat_to
                              WHERE flat_to = F.fromColumnName)

            If both are an array, add:
                  AND EXISTS (SELECT 1
                              FROM UNNEST(F.fromColumnName) AS flat_from
                              JOIN UNNEST(T.toColumnName) AS flat_to
                              ON flat_from = flat_to)
         */

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT DISTINCT '")
                .append(relationship.getToTableId())
                .append("' AS ")
                .append(PDAO_TABLE_ID_COLUMN)
                .append(", T.")
                .append(PDAO_ROW_ID_COLUMN)
                .append(" FROM `")
                .append(projectId).append('.')
                .append(datasetBqDatasetName).append('.')
                .append(relationship.getToTableName())
                .append("` AS T, `")
                .append(projectId).append('.')
                .append(datasetBqDatasetName).append('.')
                .append(relationship.getFromTableName())
                .append("` AS F, `")
                .append(projectId).append('.').append(dataSnapshotName).append('.').append(PDAO_ROW_ID_TABLE)
                .append("` AS R WHERE R.")
                .append(PDAO_TABLE_ID_COLUMN).append(" = '").append(relationship.getFromTableId())
                .append("' AND R.")
                .append(PDAO_ROW_ID_COLUMN).append(" = F.").append(PDAO_ROW_ID_COLUMN);

        String fromColumn = relationship.getFromColumnName();
        String toColumn = relationship.getToColumnName();
        Boolean fromArray = relationship.getFromColumnIsArray();
        Boolean toArray = relationship.getToColumnIsArray();

        if (fromArray && toArray) {
            builder.append(" AND EXISTS (SELECT 1 FROM UNNEST(F.")
                    .append(fromColumn)
                    .append(") AS flat_from JOIN UNNEST(T.")
                    .append(toColumn)
                    .append(") AS flat_to ON flat_from = flat_to)");
        } else if (fromArray) {
            builder.append(" AND EXISTS (SELECT 1 FROM UNNEST(F.")
                    .append(fromColumn)
                    .append(") AS flat_from WHERE flat_from = T.")
                    .append(toColumn)
                    .append(")");
        } else if (toArray) {
            builder.append(" AND EXISTS (SELECT 1 FROM UNNEST(T.")
                    .append(toColumn)
                    .append(") AS flat_to WHERE flat_to = F.")
                    .append(fromColumn)
                    .append(")");
        } else {
            builder.append(" AND T.").append(toColumn).append(" = F.").append(fromColumn);
        }

        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(sql)
                            .setDestinationTable(TableId.of(dataSnapshotName, PDAO_ROW_ID_TABLE))
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .build();

            bigQuery.query(queryConfig);
        } catch (InterruptedException ie) {
            throw new PdaoException("Append query unexpectedly interrupted", ie);
        }
    }

    private List<String> createViews(String datasetBqDatasetName, String dataSnapshotName, DataSnapshot dataSnapshot) {
        return dataSnapshot.getTables().stream().map(table -> {
                StringBuilder builder = new StringBuilder();

                /*
                  Building this SQL:
                    SELECT <column list from dataSnapshot table> FROM
                      (SELECT <column list mapping dataset to dataSnapshot columns>
                       FROM <dataset table> S, datarepo_row_ids R
                       WHERE S.datarepo_row_id = R.datarepo_row_id
                         AND R.datarepo_table_id = '<dataset table id>')
                 */

                builder.append("SELECT ");
                buildColumnList(builder, table, false);
                builder.append(" FROM ");

                // Build the FROM clause from the source
                // NOTE: we can put this in a loop when we do multiple sources
                DataSnapshotSource source = dataSnapshot.getDataSnapshotSources().get(0);
                buildSource(builder, datasetBqDatasetName, dataSnapshotName, table, source, dataSnapshot);

                // create the view
                String tableName = table.getName();
                String sql = builder.toString();

                logger.info("Creating view" + dataSnapshotName + "." + tableName + " as " + sql);
                TableId tableId = TableId.of(dataSnapshotName, tableName);
                TableInfo tableInfo = TableInfo.of(tableId, ViewDefinition.of(sql));
                com.google.cloud.bigquery.Table bqTable = bigQuery.create(tableInfo);
                return tableName;
            }
        ).collect(Collectors.toList());
    }

    private void buildColumnList(StringBuilder builder, Table table, boolean addRowIdColumn) {
        String prefix = "";
        if (addRowIdColumn) {
            builder.append(PDAO_ROW_ID_COLUMN);
            prefix = ",";
        }

        for (Column column : table.getColumns()) {
            builder.append(prefix).append(column.getName());
            prefix = ",";
        }
    }

    private void buildSource(StringBuilder builder,
                             String datasetBqDatasetName,
                             String dataSnapshotName,
                             Table table,
                             DataSnapshotSource source,
                             DataSnapshot dataSnapshot) {

        // Find the table map for the table. If there is none, we skip it.
        // NOTE: for now, we know that there will be one, because we generate it directly.
        // In the future when we have more than one, we can just return.
        DataSnapshotMapTable mapTable = lookupMapTable(table, source);
        if (mapTable == null) {
            throw new PdaoException("No matching map table for dataSnapshot table " + table.getName());
        }

        // Build this as a sub-select so it is easily extended to multiple sources in the future.
        builder.append("(SELECT ");
        buildSourceSelectList(builder, table, mapTable, dataSnapshot, source);

        builder.append(" FROM `")
                // base dataset table
                .append(projectId)
                .append('.')
                .append(datasetBqDatasetName)
                .append('.')
                .append(mapTable.getFromTable().getName())
                .append('`')
                // joined with the row id table
                .append(" S, `")
                .append(projectId)
                .append('.')
                .append(dataSnapshotName)
                .append('.')
                .append(PDAO_ROW_ID_TABLE)
                .append("` R WHERE S.")
                // where row_id matches and table_id matches
                .append(PDAO_ROW_ID_COLUMN)
                .append(" = R.")
                .append(PDAO_ROW_ID_COLUMN)
                .append(" AND R.")
                .append(PDAO_TABLE_ID_COLUMN)
                .append(" = '")
                .append(mapTable.getFromTable().getId().toString())
                .append("')");
    }

    private void buildSourceSelectList(StringBuilder builder,
                                       Table targetTable,
                                       DataSnapshotMapTable mapTable,
                                       DataSnapshot dataSnapshot,
                                       DataSnapshotSource source) {
        String prefix = "";

        for (Column targetColumn : targetTable.getColumns()) {
            builder.append(prefix);
            prefix = ",";

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

            DataSnapshotMapColumn mapColumn = lookupMapColumn(targetColumn, mapTable);
            if (mapColumn == null) {
                builder.append("NULL AS ").append(targetColumnName);
            } else if (StringUtils.equalsIgnoreCase(mapColumn.getFromColumn().getType(), "FILEREF") ||
                StringUtils.equalsIgnoreCase(mapColumn.getFromColumn().getType(), "DIRREF")) {
                if (targetColumn.isArrayOf()) {
                    // ARRAY( SELECT CONCAT('drs://datarepodnsname/v1_datasetid_datasnapshotid_', x)
                    //        FROM UNNEST(fromColumnName) AS x ) AS target
                    builder.append("ARRAY( SELECT CONCAT('drs://")
                        .append(datarepoDnsName)
                        .append("/v1_")
                        .append(source.getDataset().getId().toString())
                        .append("_")
                        .append(dataSnapshot.getId().toString())
                        .append("_',x) FROM UNNEST(")
                        .append(mapColumn.getFromColumn().getName())
                        .append(") AS x ) AS ")
                        .append(targetColumnName);
                } else {
                    // CONCAT('drs://datarepodnsname/v1_datasetid_datasnapshotid_', fromColumnName) AS target
                    builder.append("CONCAT('drs://")
                        .append(datarepoDnsName)
                        .append("/v1_")
                        .append(source.getDataset().getId().toString())
                        .append("_")
                        .append(dataSnapshot.getId().toString())
                        .append("_',")
                        .append(mapColumn.getFromColumn().getName())
                        .append(") AS ")
                        .append(targetColumnName);
                }
            } else if (StringUtils.equals(mapColumn.getFromColumn().getName(), targetColumnName)) {
                builder.append(targetColumnName);
            } else {
                builder.append(mapColumn.getFromColumn().getName())
                        .append(" AS ")
                        .append(targetColumnName);
            }
        }
    }

    private DataSnapshotMapTable lookupMapTable(Table toTable, DataSnapshotSource source) {
        for (DataSnapshotMapTable tryMapTable : source.getDataSnapshotMapTables()) {
            if (tryMapTable.getToTable().getId().equals(toTable.getId())) {
                return tryMapTable;
            }
        }
        return null;
    }

    private DataSnapshotMapColumn lookupMapColumn(Column toColumn, DataSnapshotMapTable mapTable) {
        for (DataSnapshotMapColumn tryMapColumn : mapTable.getDataSnapshotMapColumns()) {
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

}
