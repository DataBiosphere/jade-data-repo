package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;
import static bio.terra.common.PdaoConstant.PDAO_TABLE_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TEMP_TABLE;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.DateTimeUtils;
import bio.terra.common.Table;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.PdaoException;
import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.google.bq.BigQueryConfiguration;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotMapColumn;
import bio.terra.service.snapshot.SnapshotMapTable;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.MismatchedValueException;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.service.tabulardata.google.WalkRelationship;
import com.google.cloud.bigquery.Acl;
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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.ViewDefinition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

@Component
@Profile("google")
public class BigQuerySnapshotPdao {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySnapshotPdao.class);

  private final String datarepoDnsName;
  private final BigQueryConfiguration bigQueryConfiguration;

  @Autowired
  public BigQuerySnapshotPdao(
      ApplicationConfiguration applicationConfiguration,
      BigQueryConfiguration bigQueryConfiguration) {
    this.datarepoDnsName = applicationConfiguration.getDnsName();
    this.bigQueryConfiguration = bigQueryConfiguration;
  }

  private static final String loadRootRowIdsTemplate =
      "INSERT INTO `<project>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` "
          + "("
          + PDAO_TABLE_ID_COLUMN
          + ","
          + PDAO_ROW_ID_COLUMN
          + ") "
          + "SELECT '<tableId>' AS "
          + PDAO_TABLE_ID_COLUMN
          + ", T.row_id AS "
          + PDAO_ROW_ID_COLUMN
          + " FROM ("
          + "SELECT row_id FROM UNNEST([<rowIds:{id|'<id>'}; separator=\",\">]) AS row_id"
          + ") AS T";

  private static final String validateRowIdsForRootTemplate =
      "SELECT COUNT(1) FROM (<datasetLiveViewSql>) AS T, "
          + "`<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` AS R "
          + "WHERE R."
          + PDAO_ROW_ID_COLUMN
          + " = T."
          + PDAO_ROW_ID_COLUMN;

  public void createSnapshot(Snapshot snapshot, List<String> rowIds, Instant filterBefore)
      throws InterruptedException {
    BigQueryProject snapshotBigQueryProject = BigQueryProject.from(snapshot);
    String snapshotProjectId = snapshotBigQueryProject.getProjectId();

    // TODO: When we support multiple datasets per snapshot, this will need to be reworked
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(snapshot.getSourceDataset());
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
    String datasetBqDatasetName = BigQueryPdao.prefixName(source.getDataset().getName());

    AssetSpecification asset = source.getAssetSpecification();
    DatasetTable rootTable = asset.getRootTable().getTable();
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

    String datasetLiveViewSql =
        BigQueryDatasetPdao.renderDatasetLiveViewSql(
            datasetProjectId, datasetBqDatasetName, rootTable, null, filterBefore);
    ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
    sqlTemplate.add("snapshotProject", snapshotProjectId);
    sqlTemplate.add("snapshot", snapshotName);
    sqlTemplate.add("datasetLiveViewSql", datasetLiveViewSql);

    TableResult result =
        snapshotBigQueryProject.query(
            sqlTemplate.render(),
            Map.of(
                "transactionTerminatedAt",
                QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));
    FieldValueList row = result.iterateAll().iterator().next();
    FieldValue countValue = row.get(0);
    if (countValue.getLongValue() != rowIds.size()) {
      logger.error(
          "Invalid row ids supplied: rowIds="
              + rowIds.size()
              + " count="
              + countValue.getLongValue());
      for (String rowId : rowIds) {
        logger.error(" rowIdIn: " + rowId);
      }
      throw new PdaoException("Invalid row ids supplied");
    }

    // walk and populate relationship table row ids
    List<WalkRelationship> walkRelationships = WalkRelationship.ofAssetSpecification(asset);
    walkRelationships(
        datasetProjectId,
        datasetBqDatasetName,
        snapshotProjectId,
        snapshot,
        walkRelationships,
        rootTableId,
        snapshotBigQuery,
        filterBefore);

    snapshotViewCreation(
        datasetBigQueryProject,
        datasetProjectId,
        datasetBqDatasetName,
        snapshotProjectId,
        snapshot,
        snapshotBigQuery);
  }

  public void snapshotViewCreation(
      BigQueryProject datasetBigQueryProject,
      String datasetProjectId,
      String datasetBqDatasetName,
      String snapshotProjectId,
      Snapshot snapshot,
      BigQuery snapshotBigQuery)
      throws InterruptedException {
    // create the views
    List<String> bqTableNames =
        createViews(
            datasetProjectId, datasetBqDatasetName, snapshotProjectId, snapshot, snapshotBigQuery);

    // set authorization on views
    String snapshotName = snapshot.getName();
    List<Acl> acls = convertToViewAcls(snapshotProjectId, snapshotName, bqTableNames);
    datasetBigQueryProject.addDatasetAcls(datasetBqDatasetName, acls);
  }

  public void snapshotCreateBQDataset(BigQueryProject bigQueryProject, Snapshot snapshot) {
    String snapshotName = snapshot.getName();
    // Idempotency: delete possibly partial create.
    if (bigQueryProject.datasetExists(snapshotName)) {
      bigQueryProject.deleteDataset(snapshotName);
    }

    // TODO: When we support multiple datasets per snapshot, this will need to be reworked
    GoogleRegion representativeRegion =
        (GoogleRegion)
            snapshot
                .getFirstSnapshotSource()
                .getDataset()
                .getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.BIGQUERY);
    // create snapshot BQ dataset
    bigQueryProject.createDataset(snapshotName, snapshot.getDescription(), representativeRegion);
  }

  private static final String insertAllLiveViewDataTemplate =
      "INSERT INTO `<snapshotProject>.<snapshot>.<dataRepoTable>` "
          + "(<dataRepoTableId>, <dataRepoRowId>) <liveViewTables>";

  private static final String getLiveViewTableTemplate =
      // TODO pull insert out and loop thru rest w UNION ()
      "(SELECT '<tableId>', <dataRepoRowId> FROM (<datasetLiveViewSql>) AS L)";

  private static final String mergeLiveViewTablesTemplate =
      "<selectStatements; separator=\" UNION ALL \">";

  private static final String validateSnapshotSizeTemplate =
      "SELECT <rowId> FROM `<snapshotProject>.<snapshot>.<dataRepoTable>` LIMIT 1";

  public String createSnapshotTableFromLiveViews(
      BigQueryProject datasetBigQueryProject,
      List<DatasetTable> tables,
      String datasetBqDatasetName,
      Instant creationStart) {

    List<String> selectStatements = new ArrayList<>();

    for (DatasetTable table : tables) {

      ST sqlTableTemplate = new ST(getLiveViewTableTemplate);
      sqlTableTemplate.add("tableId", table.getId());
      sqlTableTemplate.add("dataRepoRowId", PDAO_ROW_ID_COLUMN);
      sqlTableTemplate.add(
          "datasetLiveViewSql",
          BigQueryDatasetPdao.renderDatasetLiveViewSql(
              datasetBigQueryProject.getProjectId(),
              datasetBqDatasetName,
              table,
              null,
              creationStart));

      selectStatements.add(sqlTableTemplate.render());
    }
    ST sqlMergeTablesTemplate = new ST(mergeLiveViewTablesTemplate);
    sqlMergeTablesTemplate.add("selectStatements", selectStatements);
    return sqlMergeTablesTemplate.render();
  }

  public void createSnapshotWithLiveViews(Snapshot snapshot, Dataset dataset, Instant filterBefore)
      throws InterruptedException {

    BigQueryProject snapshotBigQueryProject = BigQueryProject.from(snapshot);
    String snapshotProjectId = snapshotBigQueryProject.getProjectId();
    String snapshotName = snapshot.getName();
    BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();

    String datasetBqDatasetName = BigQueryPdao.prefixName(dataset.getName());
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(dataset);
    String datasetProjectId = datasetBigQueryProject.getProjectId();

    // create snapshot BQ dataset
    snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

    // create the row id table (row id col and table id col)
    snapshotBigQueryProject.createTable(snapshotName, PDAO_ROW_ID_TABLE, rowIdTableSchema());

    // get source dataset table live views
    List<DatasetTable> tables = dataset.getTables();

    // create a snapshot table based on the live view data row ids
    String liveViewTables =
        createSnapshotTableFromLiveViews(
            datasetBigQueryProject, tables, datasetBqDatasetName, filterBefore);

    ST sqlTemplate = new ST(insertAllLiveViewDataTemplate);
    sqlTemplate.add("snapshotProject", snapshotProjectId);
    sqlTemplate.add("snapshot", snapshotName);
    sqlTemplate.add("dataRepoTable", PDAO_ROW_ID_TABLE);
    sqlTemplate.add("dataRepoTableId", PDAO_TABLE_ID_COLUMN);
    sqlTemplate.add("dataRepoRowId", PDAO_ROW_ID_COLUMN);
    sqlTemplate.add("liveViewTables", liveViewTables);

    snapshotBigQueryProject.query(
        sqlTemplate.render(),
        Map.of(
            "transactionTerminatedAt",
            QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));

    ST sqlValidateSnapshotTemplate = new ST(validateSnapshotSizeTemplate);
    sqlValidateSnapshotTemplate.add("rowId", PDAO_ROW_ID_COLUMN);
    sqlValidateSnapshotTemplate.add("snapshotProject", snapshotProjectId);
    sqlValidateSnapshotTemplate.add("snapshot", snapshotName);
    sqlValidateSnapshotTemplate.add("dataRepoTable", PDAO_ROW_ID_TABLE);

    TableResult result = snapshotBigQueryProject.query(sqlValidateSnapshotTemplate.render());
    if (result.getTotalRows() <= 0) {
      throw new PdaoException("This snapshot is empty");
    }

    snapshotViewCreation(
        datasetBigQueryProject,
        datasetProjectId,
        datasetBqDatasetName,
        snapshotProjectId,
        snapshot,
        snapshotBigQuery);
  }

  public void createSnapshotWithProvidedIds(
      Snapshot snapshot, SnapshotRequestContentsModel contentsModel, Instant filterBefore)
      throws InterruptedException {

    BigQueryProject snapshotBigQueryProject = BigQueryProject.from(snapshot);
    String snapshotProjectId = snapshotBigQueryProject.getProjectId();

    // TODO: When we support multiple datasets per snapshot, this will need to be reworked
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(snapshot.getSourceDataset());
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
    String datasetBqDatasetName = BigQueryPdao.prefixName(source.getDataset().getName());

    for (SnapshotRequestRowIdTableModel table : rowIdModel.getTables()) {
      String tableName = table.getTableName();
      Table sourceTable =
          source
              .reverseTableLookup(tableName)
              .orElseThrow(
                  () -> new NotFoundException("cannot find destination table: " + tableName));

      DatasetTable datasetTable = getTable(source, tableName);

      List<UUID> rowIds = table.getRowIds();

      if (rowIds.size() > 0) {
        // we break apart the list of rowIds for better scaleability
        List<List<UUID>> rowIdChunks = ListUtils.partition(rowIds, 10000);
        // partition returns consecutive sublists of a list, each of the same size (final list may
        // be smaller)
        // partitioning a list containing [a, b, c, d, e] with a partition size of 3 yields [[a, b,
        // c], [d, e]]
        // -- an outer list containing two inner lists of three and two elements, all in the
        // original order.

        for (List<UUID> rowIdChunk :
            rowIdChunks) { // each loop will load a chunk of rowIds as an INSERT
          ST sqlTemplate = new ST(loadRootRowIdsTemplate);
          sqlTemplate.add("project", snapshotProjectId);
          sqlTemplate.add("snapshot", snapshotName);
          sqlTemplate.add("dataset", datasetBqDatasetName);
          sqlTemplate.add("tableId", sourceTable.getId().toString());
          sqlTemplate.add("rowIds", rowIdChunk);
          snapshotBigQueryProject.query(sqlTemplate.render());
        }
      }
      String datasetLiveViewSql =
          BigQueryDatasetPdao.renderDatasetLiveViewSql(
              datasetProjectId, datasetBqDatasetName, datasetTable, null, filterBefore);
      ST sqlTemplate = new ST(validateRowIdsForRootTemplate);
      sqlTemplate.add("snapshotProject", snapshotProjectId);
      sqlTemplate.add("snapshot", snapshotName);
      sqlTemplate.add("datasetLiveViewSql", datasetLiveViewSql);

      TableResult result =
          snapshotBigQueryProject.query(
              sqlTemplate.render(),
              Map.of(
                  "transactionTerminatedAt",
                  QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));
      FieldValueList row = result.iterateAll().iterator().next();
      FieldValue countValue = row.get(0);
      if (countValue.getLongValue() != rowIds.size()) {
        logger.error(
            "Invalid row ids supplied: rowIds="
                + rowIds.size()
                + " count="
                + countValue.getLongValue());
        for (UUID rowId : rowIds) {
          logger.error(" rowIdIn: " + rowId);
        }
        throw new PdaoException("Invalid row ids supplied");
      }
    }

    snapshotViewCreation(
        datasetBigQueryProject,
        datasetProjectId,
        datasetBqDatasetName,
        snapshotProjectId,
        snapshot,
        snapshotBigQuery);
  }

  public void deleteSnapshot(Snapshot snapshot) {
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    boolean snapshotTableDeleted = bigQueryProject.deleteDataset(snapshot.getName());
    logger.info("Snapshot BQ Dataset successful delete: {}", snapshotTableDeleted);
  }

  // NOTE: The CAST here should be valid for all column types but ARRAYs.
  // We validate that asset root columns are non-arrays as part of dataset creation.
  // https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules
  private static final String mapValuesToRowsTemplate =
      "SELECT T."
          + PDAO_ROW_ID_COLUMN
          + ", V.input_value FROM ("
          + "SELECT input_value FROM UNNEST([<inputVals:{v|'<v>'}; separator=\",\">]) AS input_value) AS V "
          + "LEFT JOIN (<datasetLiveViewSql>) AS T ON V.input_value = CAST(T.<column> AS STRING)";

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
  public RowIdMatch mapValuesToRows(
      SnapshotSource source, List<String> inputValues, Instant filterBefore)
      throws InterruptedException {
    // One source: grab it and navigate to the relevant parts
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(source.getDataset());
    String datasetProjectId = datasetBigQueryProject.getProjectId();
    AssetSpecification asset = source.getAssetSpecification();
    Column column = asset.getRootColumn().getDatasetColumn();
    DatasetTable datasetTable = getTable(source, column.getTable().getName());

    String datasetLiveViewSql =
        BigQueryDatasetPdao.renderDatasetLiveViewSql(
            datasetProjectId,
            BigQueryPdao.prefixName(source.getDataset().getName()),
            datasetTable,
            null,
            filterBefore);
    ST sqlTemplate = new ST(mapValuesToRowsTemplate);
    sqlTemplate.add("datasetLiveViewSql", datasetLiveViewSql);
    sqlTemplate.add("column", column.getName());
    sqlTemplate.add("inputVals", inputValues);

    // Execute the query building the row id match structure that tracks the matching
    // ids and the mismatched ids
    RowIdMatch rowIdMatch = new RowIdMatch();
    String sql = sqlTemplate.render();
    logger.debug("mapValuesToRows sql: " + sql);
    TableResult result =
        datasetBigQueryProject.query(
            sql,
            Map.of(
                "transactionTerminatedAt",
                QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));
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

  private static final String getSnapshotRefIdsTemplate =
      "SELECT <refCol> FROM `<datasetProject>.<dataset>.<table>` S, "
          + "`<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` R "
          + "<if(array)>CROSS JOIN UNNEST(S.<refCol>) AS <refCol> <endif>"
          + "WHERE S."
          + PDAO_ROW_ID_COLUMN
          + " = R."
          + PDAO_ROW_ID_COLUMN
          + " AND "
          + "R."
          + PDAO_TABLE_ID_COLUMN
          + " = '<tableId>'";

  public List<String> getSnapshotRefIds(
      Dataset dataset, Snapshot snapshot, String tableName, String tableId, Column refColumn)
      throws InterruptedException {
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(dataset);
    BigQueryProject snapshotBigQueryProject = BigQueryProject.from(snapshot);

    ST sqlTemplate = new ST(getSnapshotRefIdsTemplate);
    sqlTemplate.add("datasetProject", datasetBigQueryProject.getProjectId());
    sqlTemplate.add("snapshotProject", snapshotBigQueryProject.getProjectId());
    sqlTemplate.add("dataset", BigQueryPdao.prefixName(dataset.getName()));
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

  // insert the rowIds into the snapshot row ids table and then kick off the rest of the
  // relationship walking
  // once we have the row ids in addition to the asset spec, this should look familiar to wAsset
  public void queryForRowIds(
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException {
    // snapshot
    BigQueryProject snapshotBigQueryProject = BigQueryProject.from(snapshot);
    BigQuery snapshotBigQuery = snapshotBigQueryProject.getBigQuery();
    String snapshotProjectId = snapshotBigQueryProject.getProjectId();
    String snapshotName = snapshot.getName();

    // dataset
    // TODO: When we support multiple datasets per snapshot, this will need to be reworked
    Dataset dataset = snapshot.getSourceDataset();
    String datasetBqDatasetName = BigQueryPdao.prefixName(dataset.getName());
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(dataset);
    String datasetProjectId = datasetBigQueryProject.getProjectId();

    // TODO add additional validation that the col is the root col

    // create snapshot bq dataset
    try {

      // create snapshot BQ dataset
      snapshotCreateBQDataset(snapshotBigQueryProject, snapshot);

      // now create a temp table with all the selected row ids based on the query in it
      snapshotBigQueryProject.createTable(snapshotName, PDAO_TEMP_TABLE, tempTableSchema());

      QueryJobConfiguration queryConfig =
          QueryJobConfiguration.newBuilder(sqlQuery)
              .setDestinationTable(TableId.of(snapshotName, PDAO_TEMP_TABLE))
              .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
              .setNamedParameters(
                  Map.of(
                      "transactionTerminatedAt",
                      QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))))
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
      DatasetTable rootTable = rootAssetTable.getTable();
      String rootTableId = rootTable.getId().toString();

      ST sqlTemplate = new ST(joinTablesToTestForMissingRowIds);
      sqlTemplate.add("snapshotProject", snapshotProjectId);
      sqlTemplate.add("snapshotDatasetName", snapshotName);
      sqlTemplate.add("tempTable", PDAO_TEMP_TABLE);
      sqlTemplate.add(
          "datasetLiveViewSql",
          BigQueryDatasetPdao.renderDatasetLiveViewSql(
              datasetProjectId, datasetBqDatasetName, rootTable, null, filterBefore));
      sqlTemplate.add("commonColumn", PDAO_ROW_ID_COLUMN);

      TableResult result =
          snapshotBigQueryProject.query(
              sqlTemplate.render(),
              Map.of(
                  "transactionTerminatedAt",
                  QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));
      FieldValueList mismatchedCount = result.getValues().iterator().next();
      Long mismatchedCountLong = mismatchedCount.get(0).getLongValue();
      if (mismatchedCountLong > 0) {
        throw new MismatchedValueException("Query results did not match dataset root row ids");
      }

      // TODO should this be pulled up to the top of queryForRowIds() / added to
      // snapshotCreateBQDataset() helper
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
      sqlLoadTemplate.add(
          "commonColumn", PDAO_ROW_ID_COLUMN); // this is the disc from classic asset
      sqlLoadTemplate.add("tempTable", PDAO_TEMP_TABLE);
      snapshotBigQueryProject.query(sqlLoadTemplate.render());

      // ST sqlValidateTemplate = new ST(validateRowIdsForRootTemplate);
      // TODO do we want to reuse this validation? if yes, maybe mismatchedCount / query should be
      // updated

      // walk and populate relationship table row ids
      List<WalkRelationship> walkRelationships =
          WalkRelationship.ofAssetSpecification(assetSpecification);
      walkRelationships(
          datasetProjectId,
          datasetBqDatasetName,
          snapshotProjectId,
          snapshot,
          walkRelationships,
          rootTableId,
          snapshotBigQuery,
          filterBefore);

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

  // for each table in a dataset (source), collect row id matches ON the row id
  public RowIdMatch matchRowIds(
      SnapshotSource source, String tableName, List<UUID> rowIds, Instant filterBefore)
      throws InterruptedException {

    // One source: grab it and navigate to the relevant parts
    BigQueryProject datasetBigQueryProject = BigQueryProject.from(source.getDataset());

    Optional<SnapshotMapTable> optTable =
        source.getSnapshotMapTables().stream()
            .filter(table -> table.getFromTable().getName().equals(tableName))
            .findFirst();
    // create a column to point to the row id column in the source table to check that passed row
    // ids exist in it
    Column rowIdColumn = new Column().table(optTable.get().getFromTable()).name(PDAO_ROW_ID_COLUMN);

    // Execute the query building the row id match structure that tracks the matching
    // ids and the mismatched ids
    RowIdMatch rowIdMatch = new RowIdMatch();

    List<List<UUID>> rowIdChunks = ListUtils.partition(rowIds, 10000);
    // partition returns consecutive sublists of a list, each of the same size (final list may be
    // smaller)
    // partitioning a list containing [a, b, c, d, e] with a partition size of 3 yields [[a, b, c],
    // [d, e]]
    // -- an outer list containing two inner lists of three and two elements, all in the original
    // order.

    DatasetTable datasetTable = getTable(source, tableName);

    for (List<UUID> rowIdChunk :
        rowIdChunks) { // each loop will load a chunk of rowIds as an INSERT
      // To prevent BQ choking on a huge array, split it up into chunks
      String datasetLiveViewSql =
          BigQueryDatasetPdao.renderDatasetLiveViewSql(
              datasetBigQueryProject.getProjectId(),
              BigQueryPdao.prefixName(source.getDataset().getName()),
              datasetTable,
              null,
              filterBefore);
      ST sqlTemplate = new ST(mapValuesToRowsTemplate); // This query fails w >100k rows
      sqlTemplate.add("datasetLiveViewSql", datasetLiveViewSql);
      sqlTemplate.add("column", rowIdColumn.getName());
      sqlTemplate.add("inputVals", rowIdChunk);

      String sql = sqlTemplate.render();
      logger.debug("mapValuesToRows sql: " + sql);
      TableResult result =
          datasetBigQueryProject.query(
              sql,
              Map.of(
                  "transactionTerminatedAt",
                  QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))));
      for (FieldValueList row : result.iterateAll()) {
        // Test getting these by name
        FieldValue rowId = row.get(0);
        FieldValue inputValue = row.get(1);
        if (rowId.isNull()) {
          rowIdMatch.addMismatch(inputValue.getStringValue());
          logger.debug("rowId=<NULL>" + "  inVal=" + inputValue.getStringValue());
        } else {
          rowIdMatch.addMatch(inputValue.getStringValue(), rowId.getStringValue());
          logger.debug(
              "rowId=" + rowId.getStringValue() + "  inVal=" + inputValue.getStringValue());
        }
      }
    }

    return rowIdMatch;
  }

  public void deleteSourceDatasetViewACLs(Snapshot snapshot) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    String snapshotProjectId = bigQueryProject.getProjectId();
    List<SnapshotSource> sources = snapshot.getSnapshotSources();
    if (sources.size() > 0) {
      String datasetName = sources.get(0).getDataset().getName();
      String datasetBqDatasetName = BigQueryPdao.prefixName(datasetName);
      deleteViewAcls(datasetBqDatasetName, snapshot, snapshotProjectId);
    } else {
      logger.warn("Snapshot is missing sources: " + snapshot.getName());
    }
  }

  public boolean snapshotExists(Snapshot snapshot) throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    return bigQueryProject.datasetExists(snapshot.getName());
  }

  public void grantReadAccessToSnapshot(Snapshot snapshot, Collection<String> policies)
      throws InterruptedException {
    BigQueryPdao.grantReadAccessWorker(
        BigQueryProject.from(snapshot), snapshot.getName(), policies);
  }

  public static List<Map<String, Object>> aggregateSnapshotTable(TableResult result) {
    final FieldList columns = result.getSchema().getFields();
    final List<Map<String, Object>> values = new ArrayList<>();
    result
        .iterateAll()
        .forEach(
            rows -> {
              final Map<String, Object> rowData = new HashMap<>();
              columns.forEach(
                  column -> {
                    String columnName = column.getName();
                    FieldValue fieldValue = rows.get(columnName);
                    Object value;
                    if (fieldValue.getAttribute() == FieldValue.Attribute.REPEATED) {
                      value =
                          fieldValue.getRepeatedValue().stream()
                              .map(FieldValue::getValue)
                              .collect(Collectors.toList());
                    } else {
                      value = fieldValue.getValue();
                    }
                    rowData.put(columnName, value);
                  });
              values.add(rowData);
            });

    return values;
  }

  private static final String SNAPSHOT_DATA_TEMPLATE =
      "SELECT <columns> FROM <table> <filterParams>";

  private static final String SNAPSHOT_DATA_FILTER_TEMPLATE =
      "<whereClause> ORDER BY <sort> <direction> LIMIT <limit> OFFSET <offset>";

  /*
   * WARNING: Ensure input parameters are validated before executing this method!
   */
  public List<Map<String, Object>> getSnapshotTable(
      Snapshot snapshot,
      String tableName,
      List<String> columnNames,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter)
      throws InterruptedException {
    final BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    final String snapshotProjectId = bigQueryProject.getProjectId();
    String whereClause = StringUtils.isNotEmpty(filter) ? filter : "";

    String table = snapshot.getName() + "." + tableName;
    String columns = String.join(",", columnNames);
    // Parse before querying because the where clause is user-provided
    final String sql =
        new ST(SNAPSHOT_DATA_TEMPLATE)
            .add("columns", columns)
            .add("table", table)
            .add("filterParams", whereClause)
            .render();
    Query.parse(sql);

    // The bigquery sql table name must be enclosed in backticks
    String bigQueryTable = "`" + snapshotProjectId + "." + table + "`";
    final String filterParams =
        new ST(SNAPSHOT_DATA_FILTER_TEMPLATE)
            .add("whereClause", whereClause)
            .add("sort", sort)
            .add("direction", direction)
            .add("limit", limit)
            .add("offset", offset)
            .render();
    final String bigQuerySQL =
        new ST(SNAPSHOT_DATA_TEMPLATE)
            .add("columns", columns)
            .add("table", bigQueryTable)
            .add("filterParams", filterParams)
            .render();
    final TableResult result = bigQueryProject.query(bigQuerySQL);
    return aggregateSnapshotTable(result);
  }

  /*
   * WARNING: Ensure SQL is validated before executing this method!
   */
  public List<Map<String, Object>> getSnapshotTableUnsafe(Snapshot snapshot, String sql)
      throws InterruptedException {
    final BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);
    final TableResult result = bigQueryProject.query(sql);

    return aggregateSnapshotTable(result);
  }

  // we select from the live view here so that the row counts take into account rows that have been
  // hard deleted
  private static final String rowCountTemplate =
      "SELECT COUNT(<rowId>) FROM `<project>.<snapshot>.<table>`";

  public Map<String, Long> getSnapshotTableRowCounts(Snapshot snapshot)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(snapshot);

    Map<String, Long> rowCounts = new HashMap<>();
    for (SnapshotTable snapshotTable : snapshot.getTables()) {
      String tableName = snapshotTable.getName();
      String sql =
          new ST(rowCountTemplate)
              .add("rowId", PDAO_ROW_ID_COLUMN)
              .add("project", bigQueryProject.getProjectId())
              .add("snapshot", snapshot.getName())
              .add("table", tableName)
              .render();
      TableResult result = bigQueryProject.query(sql);
      rowCounts.put(tableName, BigQueryPdao.getSingleLongValue(result));
    }
    return rowCounts;
  }

  // HELPER METHODS

  /**
   * Recursive walk of the relationships. Note that we only follow what is connected. If there are
   * relationships in the asset that are not connected to the root, they will simply be ignored. See
   * the related comment in dataset validator.
   *
   * <p>We operate on a pdao-specific list of the asset relationships so that we can bookkeep which
   * ones we have visited. Since we need to walk relationships in both the from->to and to->from
   * direction, we have to avoid re-walking a traversed relationship or we infinite loop. Trust me,
   * I know... :)
   *
   * <p>TODO: REVIEWERS: should this code detect circular references?
   *
   * @param datasetBqDatasetName
   * @param snapshot
   * @param walkRelationships - list of relationships to consider walking
   * @param startTableId
   */
  private void walkRelationships(
      String datasetProjectId,
      String datasetBqDatasetName,
      String snapshotProjectId,
      Snapshot snapshot,
      List<WalkRelationship> walkRelationships,
      String startTableId,
      BigQuery snapshotBigQuery,
      Instant filterBefore)
      throws InterruptedException {
    for (WalkRelationship relationship : walkRelationships) {
      if (relationship.isVisited()) {
        continue;
      }

      // NOTE: setting the direction tells the WalkRelationship to change its meaning of from and
      // to.
      // When constructed, it is always in the FROM_TO direction.
      if (StringUtils.equals(startTableId, relationship.getFromTableId())) {
        relationship.setDirection(WalkRelationship.WalkDirection.FROM_TO);
      } else if (StringUtils.equals(startTableId, relationship.getToTableId())) {
        relationship.setDirection(WalkRelationship.WalkDirection.TO_FROM);
      } else {
        // This relationship is not connected to the start table
        continue;
      }
      logger.info(
          "The relationship is being set from column {} in table {} to column {} in table {}",
          relationship.getFromColumnName(),
          relationship.getFromTableName(),
          relationship.getToColumnName(),
          relationship.getToTableName());

      relationship.setVisited();
      storeRowIdsForRelatedTable(
          datasetProjectId,
          datasetBqDatasetName,
          snapshotProjectId,
          snapshot,
          relationship,
          snapshotBigQuery,
          filterBefore);
      walkRelationships(
          datasetProjectId,
          datasetBqDatasetName,
          snapshotProjectId,
          snapshot,
          walkRelationships,
          relationship.getToTableId(),
          snapshotBigQuery,
          filterBefore);
    }
  }

  // NOTE: this will have to be re-written when we support relationships that include
  // more than one column.
  private static final String storeRowIdsForRelatedTableTemplate =
      "WITH merged_table AS (SELECT DISTINCT '<toTableId>' AS "
          + PDAO_TABLE_ID_COLUMN
          + ", "
          + "T."
          + PDAO_ROW_ID_COLUMN
          + " FROM (<toTableTableSelect>) T, "
          + "(<fromTableTableSelect>) F, `<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` R "
          + "WHERE R."
          + PDAO_TABLE_ID_COLUMN
          + " = '<fromTableId>' AND "
          + "R."
          + PDAO_ROW_ID_COLUMN
          + " = F."
          + PDAO_ROW_ID_COLUMN
          + " AND F.<fromCol> = T.<toCol>) "
          + "SELECT "
          + PDAO_TABLE_ID_COLUMN
          + ","
          + PDAO_ROW_ID_COLUMN
          + " FROM merged_table WHERE "
          + PDAO_ROW_ID_COLUMN
          + " NOT IN "
          + "(SELECT "
          + PDAO_ROW_ID_COLUMN
          + " FROM `<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "`)";

  private static final String tableSelectNonArray = "(<tableSelect>)";
  private static final String tableSelectArray =
      "(SELECT <toOrFrom>0.*, FLAT_<toOrFrom> "
          + "FROM (<tableSelect>) <toOrFrom>0, "
          + "     UNNEST(<toOrFrom>0.<field>) AS FLAT_<toOrFrom>)";

  private static final String joinTablesToTestForMissingRowIds =
      "SELECT COUNT(*) FROM `<snapshotProject>.<snapshotDatasetName>.<tempTable>` AS T "
          + "LEFT JOIN (<datasetLiveViewSql>) AS D USING ( <commonColumn> ) "
          + "WHERE D.<commonColumn> IS NULL";

  private static final String loadRootRowIdsFromTempTableTemplate =
      "INSERT INTO `<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` "
          + "("
          + PDAO_TABLE_ID_COLUMN
          + ","
          + PDAO_ROW_ID_COLUMN
          + ") "
          + "SELECT '<tableId>' AS "
          + PDAO_TABLE_ID_COLUMN
          + ", T.row_id AS "
          + PDAO_ROW_ID_COLUMN
          + " FROM ("
          + "SELECT <commonColumn> AS row_id FROM `<snapshotProject>.<snapshot>.<tempTable>` "
          + ") AS T";

  /**
   * Given a relationship, join from the start table to the target table. This may be walking the
   * relationship from the from table to the to table, or walking the relationship from the to table
   * to the from table.
   *
   * @param datasetBqDatasetName - name of the dataset BigQuery dataset
   * @param snapshot - the snapshot a BigQuery dataset is being created for
   * @param relationship - relationship we are walking with its direction set. The class returns the
   *     appropriate from and to based on that direction.
   * @param datasetProjectId - the project id that this bigquery dataset exists in
   * @param snapshotProjectId - the project id that this bigquery dataset exists in
   * @param bigQuery - a BigQuery instance
   */
  private void storeRowIdsForRelatedTable(
      String datasetProjectId,
      String datasetBqDatasetName,
      String snapshotProjectId,
      Snapshot snapshot,
      WalkRelationship relationship,
      BigQuery bigQuery,
      Instant filterBefore)
      throws InterruptedException {

    DatasetTable fromTable =
        getTable(snapshot.getFirstSnapshotSource(), relationship.getFromTableName());
    DatasetTable toTable =
        getTable(snapshot.getFirstSnapshotSource(), relationship.getToTableName());
    String liveViewSqlFrom =
        BigQueryDatasetPdao.renderDatasetLiveViewSql(
            datasetProjectId, datasetBqDatasetName, fromTable, null, filterBefore);
    String liveViewSqlTo =
        BigQueryDatasetPdao.renderDatasetLiveViewSql(
            datasetProjectId, datasetBqDatasetName, toTable, null, filterBefore);

    ST fromTableTableSelect;
    ST toTableTableSelect;
    String fromCol;
    String toCol;
    if (relationship.getFromColumnIsArray() && relationship.getToColumnIsArray()) {
      fromTableTableSelect =
          new ST(tableSelectArray)
              .add("toOrFrom", "FROM")
              .add("field", relationship.getFromColumnName());
      toTableTableSelect =
          new ST(tableSelectArray)
              .add("toOrFrom", "TO")
              .add("field", relationship.getToColumnName());
      fromCol = "FLAT_FROM";
      toCol = "FLAT_TO";
    } else if (relationship.getFromColumnIsArray()) {
      fromTableTableSelect =
          new ST(tableSelectArray)
              .add("toOrFrom", "FROM")
              .add("field", relationship.getFromColumnName());
      toTableTableSelect = new ST(tableSelectNonArray);
      fromCol = "FLAT_FROM";
      toCol = relationship.getToColumnName();
    } else if (relationship.getToColumnIsArray()) {
      fromTableTableSelect = new ST(tableSelectNonArray);
      toTableTableSelect =
          new ST(tableSelectArray)
              .add("toOrFrom", "TO")
              .add("field", relationship.getToColumnName());
      fromCol = relationship.getFromColumnName();
      toCol = "FLAT_TO";
    } else {
      fromTableTableSelect = new ST(tableSelectNonArray);
      toTableTableSelect = new ST(tableSelectNonArray);
      fromCol = relationship.getFromColumnName();
      toCol = relationship.getToColumnName();
    }
    fromTableTableSelect.add("tableSelect", liveViewSqlFrom);
    toTableTableSelect.add("tableSelect", liveViewSqlTo);

    ST sqlTemplate = new ST(storeRowIdsForRelatedTableTemplate);
    sqlTemplate.add("snapshotProject", snapshotProjectId);
    sqlTemplate.add("datasetProject", datasetProjectId);
    sqlTemplate.add("dataset", datasetBqDatasetName);
    sqlTemplate.add("snapshot", snapshot.getName());
    sqlTemplate.add("fromTableId", relationship.getFromTableId());
    sqlTemplate.add("toTableId", relationship.getToTableId());
    sqlTemplate.add("fromTableTableSelect", fromTableTableSelect.render());
    sqlTemplate.add("toTableTableSelect", toTableTableSelect.render());
    sqlTemplate.add("fromCol", fromCol);
    sqlTemplate.add("toCol", toCol);

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(sqlTemplate.render())
            .setDestinationTable(TableId.of(snapshot.getName(), PDAO_ROW_ID_TABLE))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setNamedParameters(
                Map.of(
                    "transactionTerminatedAt",
                    QueryParameterValue.timestamp(DateTimeUtils.toEpochMicros(filterBefore))))
            .build();

    executeQueryWithRetry(bigQuery, queryConfig);
  }

  private SnapshotMapTable lookupMapTable(Table toTable, SnapshotSource source) {
    for (SnapshotMapTable tryMapTable : source.getSnapshotMapTables()) {
      if (tryMapTable.getToTable().getId().equals(toTable.getId())) {
        return tryMapTable;
      }
    }
    return null;
  }

  private static final String createViewsTemplate =
      "SELECT "
          + PDAO_ROW_ID_COLUMN
          + ", <columns; separator=\",\"> FROM ("
          + "SELECT S."
          + PDAO_ROW_ID_COLUMN
          + ", <mappedColumns; separator=\",\"> "
          + "FROM `<datasetProject>.<dataset>.<mapTable>` S, "
          + "`<snapshotProject>.<snapshot>."
          + PDAO_ROW_ID_TABLE
          + "` R WHERE "
          + "S."
          + PDAO_ROW_ID_COLUMN
          + " = R."
          + PDAO_ROW_ID_COLUMN
          + " AND "
          + "R."
          + PDAO_TABLE_ID_COLUMN
          + " = '<tableId>')";

  private List<String> createViews(
      String datasetProjectId,
      String datasetBqDatasetName,
      String snapshotProjectId,
      Snapshot snapshot,
      BigQuery snapshotBigQuery) {
    return snapshot.getTables().stream()
        .map(
            table -> {
              // Build the FROM clause from the source
              // NOTE: we can put this in a loop when we do multiple sources
              SnapshotSource source = snapshot.getFirstSnapshotSource();
              String snapshotName = snapshot.getName();

              // Find the table map for the table. If there is none, we skip it.
              // NOTE: for now, we know that there will be one, because we generate it directly.
              // In the future when we have more than one, we can just return.
              SnapshotMapTable mapTable = lookupMapTable(table, source);
              if (mapTable == null) {
                throw new PdaoException(
                    "No matching map table for snapshot table " + table.getName());
              }
              String snapshotId = snapshot.getId().toString();

              ST sqlTemplate = new ST(createViewsTemplate);
              sqlTemplate.add("datasetProject", datasetProjectId);
              sqlTemplate.add("snapshotProject", snapshotProjectId);
              sqlTemplate.add("dataset", datasetBqDatasetName);
              sqlTemplate.add("snapshot", snapshotName);
              sqlTemplate.add("mapTable", mapTable.getFromTable().getRawTableName());
              sqlTemplate.add("tableId", mapTable.getFromTable().getId().toString());
              table
                  .getColumns()
                  .forEach(
                      c -> {
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
            })
        .collect(Collectors.toList());
  }

  private List<Acl> convertToViewAcls(
      String projectId, String datasetName, List<String> tableNames) {
    return tableNames.stream()
        .map(tableName -> TableId.of(projectId, datasetName, tableName))
        .map(tableId -> Acl.of(new Acl.View(tableId)))
        .collect(Collectors.toList());
  }

  private DatasetTable getTable(SnapshotSource snapshotSource, String tableName) {
    return snapshotSource
        .getDataset()
        .getTableByName(tableName)
        .orElseThrow(
            () ->
                new NotFoundException(
                    String.format(
                        "Table %s was not found in dataset %s",
                        tableName, snapshotSource.getDataset().toLogString())));
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  private String sourceSelectSql(
      String snapshotId, Column targetColumn, SnapshotMapTable mapTable) {
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
      String mapName = mapColumn.getFromColumn().getName();

      if (mapColumn.getFromColumn().isFileOrDirRef()) {

        String drsPrefix = "'drs://" + datarepoDnsName + "/v1_" + snapshotId + "_'";

        if (targetColumn.isArrayOf()) {
          return "ARRAY(SELECT CONCAT("
              + drsPrefix
              + ", x) "
              + "FROM UNNEST("
              + mapName
              + ") AS x) AS "
              + targetColumnName;
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

  private SnapshotMapColumn lookupMapColumn(Column toColumn, SnapshotMapTable mapTable) {
    for (SnapshotMapColumn tryMapColumn : mapTable.getSnapshotMapColumns()) {
      if (tryMapColumn.getToColumn().getId().equals(toColumn.getId())) {
        return tryMapColumn;
      }
    }
    return null;
  }

  // Load row ids
  // We load row ids by building a SQL INSERT statement with the row ids as data.
  // This is more expensive than streaming the input, but does not introduce visibility
  // issues with the new data. It has the same number of values limitation as the
  // validate row ids.
  private String loadRowIdsSql(
      String snapshotName,
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
    builder
        .append("INSERT INTO `")
        .append(projectId)
        .append('.')
        .append(snapshotName)
        .append('.')
        .append(PDAO_ROW_ID_TABLE)
        .append("` (")
        .append(PDAO_TABLE_ID_COLUMN)
        .append(",")
        .append(PDAO_ROW_ID_COLUMN)
        .append(") SELECT ")
        .append("'")
        .append(tableId)
        .append("' AS ")
        .append(PDAO_TABLE_ID_COLUMN)
        .append(", T.rowid AS ")
        .append(PDAO_ROW_ID_COLUMN)
        .append(" FROM (SELECT rowid FROM UNNEST([");

    // Put all of the rowids into an array that is unnested into a table
    String prefix = "";
    for (String rowId : rowIds) {
      builder.append(prefix).append("'").append(rowId).append("'");
      prefix = ",";
    }

    builder
        .append("]) AS rowid EXCEPT DISTINCT ( SELECT ")
        .append(PDAO_ROW_ID_COLUMN)
        .append(" FROM `")
        .append(projectId)
        .append(".")
        .append(bqDatasetName)
        .append(".")
        .append(softDeletesTableName)
        .append("`)) AS T");
    return builder.toString();
  }

  /**
   * Check that the incoming row ids actually exist in the root table.
   *
   * <p>Even though these are currently generated within the create snapshot flight, they may be
   * exposed externally in the future, so validating seemed like a good idea. At this point, the
   * only thing we have stored into the row id table are the incoming row ids. We make the equi-join
   * of row id table and root table over row id. We should get one root table row for each row id
   * table row. So we validate by comparing the count of the joined rows against the count of
   * incoming row ids. This will catch duplicate and mismatched row ids.
   *
   * @param datasetBqDatasetName
   * @param snapshotName
   * @param rootTableName
   * @param projectId
   */
  private String validateRowIdsForRootSql(
      String datasetBqDatasetName, String snapshotName, String rootTableName, String projectId) {
    StringBuilder builder = new StringBuilder();
    builder
        .append("SELECT COUNT(*) FROM `")
        .append(projectId)
        .append('.')
        .append(datasetBqDatasetName)
        .append('.')
        .append(rootTableName)
        .append("` AS T, `")
        .append(projectId)
        .append('.')
        .append(snapshotName)
        .append('.')
        .append(PDAO_ROW_ID_TABLE)
        .append("` AS R WHERE R.")
        .append(PDAO_ROW_ID_COLUMN)
        .append(" = T.")
        .append(PDAO_ROW_ID_COLUMN);
    return builder.toString();
  }

  private void deleteViewAcls(
      String datasetBqDatasetName, Snapshot snapshot, String snapshotProjectId)
      throws InterruptedException {
    BigQueryProject bigQueryDatasetProject = BigQueryProject.from(snapshot.getSourceDataset());

    List<String> viewsToDelete =
        snapshot.getTables().stream()
            .map(
                table -> {
                  // Build the FROM clause from the source
                  // NOTE: we can put this in a loop when we do multiple sources
                  SnapshotSource source = snapshot.getFirstSnapshotSource();

                  // Find the table map for the table. If there is none, we skip it.
                  // NOTE: for now, we know that there will be one, because we generate it directly.
                  // In the future when we have more than one, we can just return.
                  SnapshotMapTable mapTable = lookupMapTable(table, source);
                  if (mapTable == null) {
                    throw new PdaoException(
                        "No matching map table for snapshot table " + table.getName());
                  }
                  // get the list of table names
                  return table.getName();
                })
            .collect(Collectors.toList());

    // delete the view Acls
    String snapshotName = snapshot.getName();
    viewsToDelete.forEach(
        tableName -> logger.info("Deleting ACLs for view " + snapshotName + "." + tableName));
    List<Acl> acls = convertToViewAcls(snapshotProjectId, snapshotName, viewsToDelete);
    bigQueryDatasetProject.removeDatasetAcls(datasetBqDatasetName, acls);
  }

  private Schema tempTableSchema() {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
    return Schema.of(fieldList);
  }

  /**
   * Run query up to a predetermined number of times until it's first success if the failed is a
   * rate limit exception.
   */
  private TableResult executeQueryWithRetry(
      final BigQuery bigQuery, final QueryJobConfiguration queryConfig)
      throws InterruptedException {
    return executeQueryWithRetry(bigQuery, queryConfig, 0);
  }

  /**
   * Run query up to a predetermined number of times until its first success if the failed is a rate
   * limit exception.
   */
  private TableResult executeQueryWithRetry(
      final BigQuery bigQuery, final QueryJobConfiguration queryConfig, final int retryNum)
      throws InterruptedException {
    final int maxRetries = bigQueryConfiguration.getRateLimitRetries();
    final int retryWait = bigQueryConfiguration.getRateLimitRetryWaitMs();

    if (retryNum > 0) {
      logger.info("Retry number {} of a maximum {}", retryNum, maxRetries);
    }
    try {
      logQuery(queryConfig);
      return bigQuery.query(queryConfig);
    } catch (final BigQueryException qe) {
      if (qe.getError() != null
          && Objects.equals(qe.getError().getReason(), "jobRateLimitExceeded")
          && retryNum <= maxRetries) {
        logger.warn(
            "Query failed to run due to exceeding the BigQuery update rate limit.  Retrying.");
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

  // SCHEMA BUILDERS

  private Schema rowIdTableSchema() {
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(Field.of(PDAO_TABLE_ID_COLUMN, LegacySQLTypeName.STRING));
    fieldList.add(Field.of(PDAO_ROW_ID_COLUMN, LegacySQLTypeName.STRING));
    return Schema.of(fieldList);
  }

  public static void logQuery(QueryJobConfiguration queryConfig) {
    logger.info(
        "Running query:\n#########\n{}\n#########\nwith parameters {}",
        queryConfig.getQuery(),
        queryConfig.getNamedParameters());
  }
}
