package bio.terra.pdao.bigquery;

import bio.terra.flight.exception.IngestFailureException;
import bio.terra.flight.exception.IngestInterruptedException;
import bio.terra.metadata.AssetSpecification;
import bio.terra.metadata.Column;
import bio.terra.metadata.DatasetMapColumn;
import bio.terra.metadata.DatasetMapTable;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.RowIdMatch;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoException;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.PrimaryDataAccess;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static bio.terra.pdao.PdaoConstant.PDAO_PREFIX;
import static bio.terra.pdao.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.pdao.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.pdao.PdaoConstant.PDAO_TABLE_ID_COLUMN;

@Component
@Profile("bigquery")
public class BigQueryPdao implements PrimaryDataAccess {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.pdao.bigquery");

    private final BigQuery bigQuery;
    private final String projectId;

    @Autowired
    public BigQueryPdao(BigQuery bigQuery, String bigQueryProjectId) {
        this.bigQuery = bigQuery;
        this.projectId = bigQueryProjectId;
    }

    @Override
    public boolean studyExists(String name) {
        return existenceCheck(prefixName(name));
    }

    @Override
    public void createStudy(Study study) {
        // Keep the study name from colliding with a dataset name by prefixing it.
        String studyName = prefixName(study.getName());
        try {
            // For idempotency, if we find the study exists, we assume that we started to
            // create it before and failed in the middle. We delete it and re-create it from scratch.
            if (studyExists(study.getName())) {
                deleteStudy(study);
            }

            createContainer(studyName, study.getDescription());
            for (Table table : study.getTables()) {
                createTable(studyName, table, table.getName());
            }
        } catch (Exception ex) {
            throw new PdaoException("create study failed for " + studyName, ex);
        }
    }

    @Override
    public boolean deleteStudy(Study study) {
        return deleteContainer(prefixName(study.getName()));
    }

    @Override
    public boolean datasetExists(String datasetName) {
        return existenceCheck(prefixName(datasetName));
    }

    // compute the row ids from the input ids and validate all inputs have matches
    // Add new public method that takes the asset and the dataset source and the input values and
    // returns a structure with the matching row ids (suitable for calling create dataset)
    // and any mismatched input values that don't have corresponding roww.
    // NOTE: In the fullness of time, we may not do this and kick the function into the UI.
    // So this code assumes there is one source and one set of input values.
    // The query it builds embeds data values into the query in an array. I think it will
    // support about 25,000 input values. It that is not enough there is another, more
    // complicated alternative:
    // - create a scratch table at dataset creation time
    // - truncate before we start
    // - load the values in
    // - do the query
    // - truncate (even tidier...)
    // So if we need to make this work in the long term, we can take that approach.
    @Override
    public RowIdMatch mapValuesToRows(bio.terra.metadata.Dataset dataset,
                                      DatasetSource source,
                                      List<String> inputValues) {
        /*
            Making this SQL query:
            SELECT T.datarepo_row_id, T.<study-column>, V.inputValue
            FROM (select inputValue from unnest(['inputValue0', 'inputValue1', ...]) as inputValue) AS V
            LEFT JOIN <study-table> AS T
            ON T.<study-column> = V.inputValue
        */

        // One source: grab it and navigate to the relevant parts
        AssetSpecification asset = source.getAssetSpecification();
        Column column = asset.getRootColumn().getStudyColumn();
        String studyColumnName = column.getName();
        String studyTableName = column.getTable().getName();
        String studyDatasetName = prefixName(source.getStudy().getName());

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
                .append(studyDatasetName)
                .append('.')
                .append(studyTableName)
                .append("` AS T ON V.inputValue = T.")
                .append(studyColumnName);

        // Execute the query building the row id match structure that tracks the matching
        // ids and the mismatched ids
        RowIdMatch rowIdMatch = new RowIdMatch();
        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
            for (FieldValueList row : result.iterateAll()) {
                // Test getting these by name
                FieldValue rowId = row.get(0);
                FieldValue inputValue = row.get(1);
                if (rowId.isNull()) {
                    rowIdMatch.addMismatch(inputValue.getStringValue());
                } else {
                    rowIdMatch.addMatch(inputValue.getStringValue(), rowId.getStringValue());
                }
            }
        } catch (InterruptedException ie) {
            throw new PdaoException("Append query unexpectedly interrupted", ie);
        }

        return rowIdMatch;
    }

    @Override
    public void createDataset(bio.terra.metadata.Dataset dataset, List<String> rowIds) {
        String datasetName = dataset.getName();
        try {
            // Idempotency: delete possibly partial create.
            if (existenceCheck(datasetName)) {
                deleteContainer(datasetName);
            }

            createContainer(datasetName, dataset.getDescription());

            // create the row id table
            createRowIdTable(datasetName);

            // populate root row ids. Must happen before the relationship walk.
            // NOTE: when we have multiple sources, we can put this into a loop
            DatasetSource source = dataset.getDatasetSources().get(0);
            String studyDatasetName = prefixName(source.getStudy().getName());

            storeRowIdsForRoot(studyDatasetName, datasetName, source, rowIds);

            // walk and populate relationship table row ids
            AssetSpecification asset = source.getAssetSpecification();
            String rootTableId = asset.getRootTable().getTable().getId().toString();
            List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(asset);

            walkRelationships(studyDatasetName, datasetName, walkRelationships, rootTableId);

            // create the views
            createViews(studyDatasetName, datasetName, dataset);

        } catch (Exception ex) {
            throw new PdaoException("createDataset failed", ex);
        }
    }

    @Override
    public boolean deleteDataset(bio.terra.metadata.Dataset dataset) {
        return deleteContainer(dataset.getName());
    }

    // Create a staging table for loading data
    public void createStagingTable(String studyName,
                                   Table targetTable,
                                   String stagingTableName) {
        String containerName = prefixName(studyName);
        createTable(containerName, targetTable, stagingTableName);
    }

    // Load data
    public PdaoLoadStatistics loadToStagingTable(Study study,
                                                 Table targetTable,
                                                 String stagingTableName,
                                                 IngestRequestModel ingestRequest) {

        TableId tableId = TableId.of(prefixName(study.getName()), stagingTableName);
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
            // TODO: DR-263:
            //List<BigQueryError> errors = loadJob.getStatus().getExecutionErrors();
            // For now, we just throw an error based on the last error reported from the
            // load job.
            // TODO: also, understand what reasons are. Some might be retryable by Stairway.
            // Others are not, like bad input data.
            BigQueryError lastError = loadJob.getStatus().getError();
            throw new IngestFailureException("Ingest failed: " + lastError.toString());
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

    public void addRowIdsToStagingTable(Study study, String stagingTableName) {
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
            .append(prefixName(study.getName()))
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

    public void insertIntoStudyTable(Study study,
                                     Table targetTable,
                                     String stagingTableName) {
        /*
         * INSERT INTO `project.dataset.studytable`
         * (<column names...>)
         * SELECT <column names...>
         * FROM `project.dataset.studytable`
         */
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT ")
            .append("`")
            .append(projectId)
            .append(".")
            .append(prefixName(study.getName()))
            .append(".")
            .append(targetTable.getName())
            .append("` (");
        buildColumnList(sql, targetTable, true);
        sql.append(") SELECT ");
        buildColumnList(sql, targetTable, true);
        sql.append(" FROM `")
            .append(projectId)
            .append(".")
            .append(prefixName(study.getName()))
            .append(".")
            .append(stagingTableName)
            .append("`");

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql.toString()).build();
            bigQuery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Insert staging into study table unexpectedly interrupted", e);
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



    public boolean deleteTable(String studyName, String tableName) {
        try {
            TableId tableId = TableId.of(projectId, prefixName(studyName), tableName);
            return bigQuery.delete(tableId);
        } catch (Exception ex) {
            throw new PdaoException("Failed attempt delete of study: " +
                studyName + " table: " + tableName, ex);
        }
    }


    private boolean existenceCheck(String name) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, name);
            Dataset dataset = bigQuery.getDataset(datasetId);
            return (dataset != null);
        } catch (Exception ex) {
            throw new PdaoException("existence check failed for " + name, ex);
        }
    }

    private String prefixName(String name) {
        return PDAO_PREFIX + name;
    }

    private void createContainer(String name, String description) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(name)
            .setDescription(description)
            .build();
        bigQuery.create(datasetInfo);
    }

    private boolean deleteContainer(String name) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, name);
            return bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
        } catch (Exception ex) {
            throw new PdaoException("delete failed for " + name, ex);
        }
    }

    private void createTable(String containerName, Table table, String tableName) {
        TableId tableId = TableId.of(containerName, tableName);
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

    private void createRowIdTable(String datasetName) {
        TableId tableId = TableId.of(datasetName, PDAO_ROW_ID_TABLE);
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING));
        fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
        Schema schema = Schema.of(fieldList);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    private void storeRowIdsForRoot(String studyDatasetName,
                                    String datasetName,
                                    DatasetSource source,
                                    List<String> rowIds) {
        AssetSpecification asset = source.getAssetSpecification();
        Table rootTable = asset.getRootTable().getTable();
        loadRootRowIds(datasetName, rootTable.getId().toString(), rowIds);
        validateRowIdsForRoot(studyDatasetName, datasetName, rootTable.getName(), rowIds);
    }

    // Load row ids
    private void loadRootRowIds(String datasetName, String tableId, List<String> rowIds) {
        if (rowIds.size() == 0) {
            return;
        }

        TableId bqTableId = TableId.of(datasetName, PDAO_ROW_ID_TABLE);
        InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(bqTableId);
        for (String rowId : rowIds) {
            Map<String, Object> row = new HashMap<>();
            row.put(PDAO_TABLE_ID_COLUMN, tableId);
            row.put(PDAO_ROW_ID_COLUMN, rowId);
            requestBuilder.addRow(row);
        }
        InsertAllRequest request = requestBuilder.build();

        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {
            logger.error("Row Id InsertAll had errors:");
            for (List<BigQueryError> errorList : response.getInsertErrors().values()) {
                for (BigQueryError bigQueryError : errorList) {
                    logger.debug("    {}", bigQueryError);
                }
            }
        }
    }

    /**
     * Check that the incoming row ids actually exist in the root table.
     *
     * Even though these are currently generated within the create dataset flight, they may
     * be exposed externally in the future, so validating seemed like a good idea.
     * At this point, the only thing we have stored into the row id table are the incoming row ids.
     * We make the equi-join of row id table and root table over row id. We should get one root table row
     * for each row id table row. So we validate by comparing the count of the joined rows against the
     * count of incoming row ids. This will catch duplicate and mismatched row ids.
     *
     * @param studyDatasetName
     * @param datasetName
     * @param rootTableName
     * @param rowIds
     */
    private void validateRowIdsForRoot(String studyDatasetName,
                                       String datasetName,
                                       String rootTableName,
                                       List<String> rowIds) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(projectId).append('.').append(studyDatasetName).append('.').append(rootTableName)
                .append("` AS T, `")
                .append(projectId).append('.').append(datasetName).append('.').append(PDAO_ROW_ID_TABLE)
                .append("` AS R WHERE R.")
                .append(PDAO_ROW_ID_COLUMN).append(" = T.").append(PDAO_ROW_ID_COLUMN);

        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            TableResult result = bigQuery.query(queryConfig);
            FieldValueList row = result.iterateAll().iterator().next();
            FieldValue countValue = row.get(0);
            if (countValue.getLongValue() != rowIds.size()) {
                throw new PdaoException("Invalid row ids supplied");
            }
        } catch (InterruptedException ie) {
            throw new PdaoException("Validate row ids query unexpectedly interrupted", ie);
        }
    }

    /**
     * Recursive walk of the relationships. Note that we only follow what is connected.
     * If there are relationships in the asset that are not connected to the root, they will
     * simply be ignored. See the related comment in study validator.
     *
     * We operate on a pdoa-specific list of the asset relationships so that we can
     * bookkeep which ones we have visited. Since we need to walk relationships in both
     * the from->to and to->from direction, we have to avoid re-walking a traversed relationship
     * or we infinite loop. Trust me, I know... :)
     *
     * TODO: REVIEWERS: should this code detect circular references?
     *
     * @param studyDatasetName
     * @param datasetName
     * @param walkRelationships - list of relationships to consider walking
     * @param startTableId
     */
    private void walkRelationships(String studyDatasetName,
                                   String datasetName,
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
            storeRowIdsForRelatedTable(studyDatasetName, datasetName, relationship);
            walkRelationships(studyDatasetName, datasetName, walkRelationships, relationship.getToTableId());
        }
    }

    /**
     * Given a relationship, join from the start table to the target table.
     * This may be walking the relationship from the from table to the to table,
     * or walking the relationship from the to table to the from table.
     *
     * @param studyDatasetName - name of the study BigQuery dataset
     * @param datasetName - name of the new dataset's BigQuery dataset
     * @param relationship - relationship we are walking with its direction set. The class returns
     *                       the appropriate from and to based on that direction.
     */
    private void storeRowIdsForRelatedTable(String studyDatasetName,
                                            String datasetName,
                                            WalkRelationship relationship) {
        // NOTE: this will have to be re-written when we support relationships that include
        // more than one column.
        /*
            Building this SQL:
                SELECT DISTINCT 'toTableId' AS PDAO_TABLE_ID_COLUMN, T.PDAO_ROW_ID_COLUMN
                FROM toTableName T, fromTableName F, ROW_ID_TABLE_NAME R
                WHERE R.PDAO_TABLE_ID_COLUMN = 'fromTableId'
                  AND R.PDAO_ROW_ID_COLUMN = F.PDAO_ROW_ID_COLUMN
                  AND T.toColumnName = F.fromColumnName
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
                .append(studyDatasetName).append('.')
                .append(relationship.getToTableName())
                .append("` AS T, `")
                .append(projectId).append('.')
                .append(studyDatasetName).append('.')
                .append(relationship.getFromTableName())
                .append("` AS F, `")
                .append(projectId).append('.').append(datasetName).append('.').append(PDAO_ROW_ID_TABLE)
                .append("` AS R WHERE R.")
                .append(PDAO_TABLE_ID_COLUMN).append(" = '").append(relationship.getFromTableId())
                .append("' AND R.")
                .append(PDAO_ROW_ID_COLUMN).append(" = F.").append(PDAO_ROW_ID_COLUMN)
                .append(" AND T.").append(relationship.getToColumnName())
                .append(" = F.").append(relationship.getFromColumnName());

        String sql = builder.toString();
        try {
            QueryJobConfiguration queryConfig =
                    QueryJobConfiguration.newBuilder(sql)
                            .setDestinationTable(TableId.of(datasetName, PDAO_ROW_ID_TABLE))
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                            .build();

            bigQuery.query(queryConfig);
        } catch (InterruptedException ie) {
            throw new PdaoException("Append query unexpectedly interrupted", ie);
        }
    }

    private void createViews(String studyDatasetName, String datasetName, bio.terra.metadata.Dataset dataset) {
        for (Table table : dataset.getTables()) {
            StringBuilder builder = new StringBuilder();

            /*
            Building this SQL:
                SELECT <column list from dataset table> FROM
                  (SELECT <column list mapping study to dataset columns>
                   FROM <study table> S, datarepo_row_ids R
                   WHERE S.datarepo_row_id = R.datarepo_row_id
                     AND R.datarepo_table_id = '<study table id>')
             */

            builder.append("SELECT ");
            buildColumnList(builder, table, false);
            builder.append(" FROM ");

            // Build the FROM clause from the source
            // NOTE: we can put this in a loop when we do multiple sources
            DatasetSource source = dataset.getDatasetSources().get(0);
            buildSource(builder, studyDatasetName, datasetName, table, source);

            // create the view
            String tableName = table.getName();
            String sql = builder.toString();

            logger.debug("Creating view" + datasetName + "." + tableName + " as " + sql);
            TableId tableId = TableId.of(datasetName, tableName);
            TableInfo tableInfo = TableInfo.of(tableId, ViewDefinition.of(sql));
            bigQuery.create(tableInfo);
        }
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
                             String studyDatasetName,
                             String datasetName,
                             Table table,
                             DatasetSource source) {

        // Find the table map for the table. If there is none, we skip it.
        // NOTE: for now, we know that there will be one, because we generate it directly.
        // In the future when we have more than one, we can just return.
        DatasetMapTable mapTable = lookupMapTable(table, source);
        if (mapTable == null) {
            throw new PdaoException("No matching map table for dataset table " + table.getName());
        }

        // Build this as a sub-select so it is easily extended to multiple sources in the future.
        builder.append("(SELECT ");
        buildSourceSelectList(builder, table, mapTable);

        builder.append(" FROM `")
                // base study table
                .append(projectId)
                .append('.')
                .append(studyDatasetName)
                .append('.')
                .append(mapTable.getFromTable().getName())
                .append('`')
                // joined with the row id table
                .append(" S, `")
                .append(projectId)
                .append('.')
                .append(datasetName)
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

    private void buildSourceSelectList(StringBuilder builder, Table targetTable, DatasetMapTable mapTable) {
        String prefix = "";

        for (Column targetColumn : targetTable.getColumns()) {
            builder.append(prefix);
            prefix = ",";

            // In the future, there may not be a column map for a given target column; it might not exist
            // in the table. The logic here covers three cases:
            // 1) no source column: supply NULL
            // 2) source and target column with same name: supply name
            // 3) source and target column with different names: supply target name with AS
            String targetColumnName = targetColumn.getName();

            DatasetMapColumn mapColumn = lookupMapColumn(targetColumn, mapTable);
            if (mapColumn == null) {
                builder.append("NULL AS ").append(targetColumnName);
            } else if (StringUtils.equals(mapColumn.getFromColumn().getName(), targetColumnName)) {
                builder.append(targetColumnName);
            } else {
                builder.append(mapColumn.getFromColumn().getName())
                        .append(" AS ")
                        .append(targetColumnName);
            }
        }
    }

    private DatasetMapTable lookupMapTable(Table toTable, DatasetSource source) {
        for (DatasetMapTable tryMapTable : source.getDatasetMapTables()) {
            if (tryMapTable.getToTable().getId().equals(toTable.getId())) {
                return tryMapTable;
            }
        }
        return null;
    }

    private DatasetMapColumn lookupMapColumn(Column toColumn, DatasetMapTable mapTable) {
        for (DatasetMapColumn tryMapColumn : mapTable.getDatasetMapColumns()) {
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
