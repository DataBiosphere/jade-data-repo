package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_COUNT_ALIAS;
import static bio.terra.common.PdaoConstant.PDAO_COUNT_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_FILTERED_ROW_COUNT_COLUMN_NAME;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static bio.terra.common.PdaoConstant.PDAO_TOTAL_ROW_COUNT_COLUMN_NAME;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.exception.PdaoException;
import bio.terra.grammar.Query;
import bio.terra.model.ColumnStatisticsDoubleModel;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.model.ColumnStatisticsTextModel;
import bio.terra.model.ColumnStatisticsTextValue;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

public abstract class BigQueryPdao {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryPdao.class);

  static void grantReadAccessWorker(
      BigQueryProject bigQueryProject, String name, Collection<String> policyGroupEmails)
      throws InterruptedException {
    List<Acl> policyGroupAcls =
        policyGroupEmails.stream()
            .map(email -> Acl.of(new Acl.Group(email), Acl.Role.READER))
            .collect(Collectors.toList());
    bigQueryProject.addDatasetAcls(name, policyGroupAcls);
  }

  private static final String selectHasDuplicateStagingIdsTemplate =
      "SELECT <pkColumns:{c|<c.name>}; separator=\",\">,COUNT(*) AS <count> "
          + "FROM `<project>.<dataset>.<tableName>` "
          + "GROUP BY <pkColumns:{c|<c.name>}; separator=\",\"> "
          + "HAVING COUNT(*) > 1";

  /**
   * Returns true is any duplicate IDs are present in a BigQuery table TODO: add support for
   * returning top few instances
   */
  public static TableResult duplicatePrimaryKeys(
      FSContainerInterface container, List<Column> pkColumns, String tableName)
      throws InterruptedException {
    BigQueryProject bigQueryProject = BigQueryProject.from(container);

    String bqDatasetName = prefixContainerName(container);

    ST sqlTemplate =
        new ST(selectHasDuplicateStagingIdsTemplate)
            .add("count", PDAO_COUNT_ALIAS)
            .add("project", bigQueryProject.getProjectId())
            .add("dataset", bqDatasetName)
            .add("tableName", tableName)
            .add("pkColumns", pkColumns);

    return bigQueryProject.query(sqlTemplate.render());
  }

  public static String prefixName(String name) {
    return PDAO_PREFIX + name;
  }

  static String externalTableName(String tableName, String suffix) {
    return String.format("%s%s_%s", PDAO_EXTERNAL_TABLE_PREFIX, tableName, suffix);
  }

  public static boolean deleteExternalTable(
      FSContainerInterface container, String tableName, String suffix) {

    BigQueryProject bigQueryProject = BigQueryProject.from(container);
    String bqDatasetName = prefixContainerName(container);
    String extTableName = externalTableName(tableName, suffix);
    return bigQueryProject.deleteTable(bqDatasetName, extTableName);
  }

  private static String prefixContainerName(FSContainerInterface container) {
    if (container.getCollectionType() == CollectionType.DATASET) {
      return prefixName(container.getName());
    } else {
      return container.getName();
    }
  }

  static long getSingleLongValue(TableResult result) {
    FieldValueList fieldValues = result.getValues().iterator().next();
    return fieldValues.get(0).getLongValue();
  }

  public static boolean tooManyDmlStatementsOutstanding(PdaoException ex) {
    return ex.getCause() instanceof BigQueryException
        && (ex.getCause().getMessage().contains("Too many DML statements outstanding against table")
            || ((BigQueryException) ex.getCause()).getReason().contains("jobRateLimitExceeded"));
  }

  // VIEW DATA
  public static final String DATA_TEMPLATE =
      """
        SELECT <columns>,
          <if(includeTotalRowCount)>
            <totalRowCountColumnName>,
          <endif>
          count(*) over () <filteredRowCountColumnName>
        FROM (
          SELECT <pdaoRowIdColumn><columns><if(includeTotalRowCount)>, count(*) over () AS <totalRowCountColumnName><endif>
          FROM <table>)
        <filterParams>
      """;

  public static final String DATA_FILTER_TEMPLATE =
      "<whereClause> ORDER BY <sort> <direction> LIMIT <limit> OFFSET <offset>";

  public static final String TABLE_ROW_COUNT_TEMPLATE =
      """
        SELECT count(*) <totalRowCountColumnName> FROM <table>
      """;

  // The bigquery sql table name must be enclosed in backticks
  public static final String BQ_TABLE_NAME_TEMPLATE =
      "`<projectId>.<pdaoPrefix><resourceName>.<tableName>`";

  public static String bqTableName(FSContainerInterface tdrResource, String tableName) {
    final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
    final String projectId = bigQueryProject.getProjectId();
    return new ST(BQ_TABLE_NAME_TEMPLATE)
        .add("projectId", projectId)
        .add("pdaoPrefix", tdrResource.isDataset() ? PDAO_PREFIX : "")
        .add("resourceName", tdrResource.getName())
        .add("tableName", tableName)
        .render();
  }

  public static int getTableTotalRowCount(FSContainerInterface tdrResource, String tableName) {
    final String bigQuerySQL =
        new ST(TABLE_ROW_COUNT_TEMPLATE)
            .add("table", bqTableName(tdrResource, tableName))
            .add("totalRowCountColumnName", PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)
            .render();
    try {
      final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
      final TableResult result = bigQueryProject.query(bigQuerySQL);
      return getIntResult(result, PDAO_TOTAL_ROW_COUNT_COLUMN_NAME);
    } catch (InterruptedException ex) {
      logger.warn(
          "BQ request to get total row count for table {} was interupted. Defaulting to 0.",
          tableName,
          ex);
      return 0;
    }
  }
  /*
   * WARNING: Ensure input parameters are validated before executing this method!
   */
  public static List<BigQueryDataResultModel> getTable(
      FSContainerInterface tdrResource,
      String tableName,
      List<String> columnNames,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter)
      throws InterruptedException {
    String whereClause = StringUtils.isNotEmpty(filter) ? filter : "";
    boolean isDataset = tdrResource.getCollectionType().equals(CollectionType.DATASET);

    String columns = String.join(",", columnNames);
    // Parse before querying because the where clause is user-provided
    // TODO - This code should be shared with Azure equivalent call (DR-2937)
    final String sql =
        new ST(DATA_TEMPLATE)
            .add("columns", columns)
            .add("table", bqTableName(tdrResource, tableName))
            .add("filterParams", whereClause)
            .add("includeTotalRowCount", isDataset)
            .add("totalRowCountColumnName", PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)
            .add("filteredRowCountColumnName", PDAO_FILTERED_ROW_COUNT_COLUMN_NAME)
            .add(
                "pdaoRowIdColumn",
                columnNames.contains(PDAO_ROW_ID_COLUMN) ? "" : PDAO_ROW_ID_COLUMN + ",")
            .render();
    Query.parse(sql);

    // The bigquery sql table name must be enclosed in backticks
    final String filterParams =
        new ST(DATA_FILTER_TEMPLATE)
            .add("whereClause", whereClause)
            .add("sort", sort)
            .add("direction", direction)
            .add("limit", limit)
            .add("offset", offset)
            .render();
    final String bigQuerySQL =
        new ST(DATA_TEMPLATE)
            .add("columns", columns)
            .add("table", bqTableName(tdrResource, tableName))
            .add("filterParams", filterParams)
            .add("includeTotalRowCount", isDataset)
            .add("totalRowCountColumnName", PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)
            .add("filteredRowCountColumnName", PDAO_FILTERED_ROW_COUNT_COLUMN_NAME)
            .add(
                "pdaoRowIdColumn",
                columnNames.contains(PDAO_ROW_ID_COLUMN) ? "" : PDAO_ROW_ID_COLUMN + ",")
            .render();
    final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
    final TableResult result = bigQueryProject.query(bigQuerySQL);
    return aggregateTableData(result);
  }

  public static List<BigQueryDataResultModel> aggregateTableData(TableResult result) {
    FieldList columns = result.getSchema().getFields();
    final List<BigQueryDataResultModel> values = new ArrayList<>();
    result
        .iterateAll()
        .forEach(
            rows -> {
              final BigQueryDataResultModel resultModel = new BigQueryDataResultModel();
              final Map<String, Object> rowData = new HashMap<>();
              columns.forEach(
                  column -> {
                    String columnName = column.getName();
                    FieldValue fieldValue = rows.get(columnName);
                    Object value;
                    if (columnName.equals(PDAO_FILTERED_ROW_COUNT_COLUMN_NAME)) {
                      resultModel.filteredCount((int) fieldValue.getLongValue());
                    } else if (columnName.equals(PDAO_TOTAL_ROW_COUNT_COLUMN_NAME)) {
                      resultModel.totalCount((int) fieldValue.getLongValue());
                    } else {
                      if (fieldValue.getAttribute() == FieldValue.Attribute.REPEATED) {
                        value =
                            fieldValue.getRepeatedValue().stream()
                                .map(FieldValue::getValue)
                                .collect(Collectors.toList());
                      } else {
                        value = fieldValue.getValue();
                      }
                      rowData.put(columnName, value);
                    }
                  });
              values.add(resultModel.rowResult(rowData));
            });
    return values;
  }

  // COLUMN STATS
  public static final String ARRAY_TEXT_COLUMN_STATS_TEMPLATE =
      """
          WITH array_field AS (SELECT <column> FROM <table> <whereClause>)
            SELECT flattened_array_field AS <column>, COUNT(*) AS <countColumn> FROM array_field CROSS JOIN UNNEST(array_field.<column>)
            AS flattened_array_field GROUP BY flattened_array_field ORDER BY flattened_array_field <direction>
          """;
  public static final String TEXT_COLUMN_STATS_TEMPLATE =
      """
        SELECT <column>, COUNT(*) AS <countColumn> FROM <table> AS <tableName> <whereClause> GROUP BY <tableName>.<column> ORDER BY <column> <direction>
      """;

  public static final String ARRAY_NUMERIC_COLUMN_STATS_TEMPLATE =
      """
          WITH array_field AS (SELECT <column> FROM <table> <whereClause>)
            SELECT MIN(flattened_array_field) AS min, MAX(flattened_array_field) AS max FROM array_field CROSS JOIN UNNEST(array_field.<column>)
            AS flattened_array_field
          """;

  public static final String NUMERIC_COLUMN_STATS_TEMPLATE =
      """
        SELECT MIN(<column>) AS min, MAX(<column>) AS max FROM <table> <whereClause>
      """;

  public static ColumnStatisticsTextModel getStatsForTextColumn(
      FSContainerInterface tdrResource, String tableName, Column column, String filter)
      throws InterruptedException {
    String whereClause = StringUtils.isNotEmpty(filter) ? filter : "";
    final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
    String columnName = column.getName();
    final String bigQuerySQL =
        new ST(column.isArrayOf() ? ARRAY_TEXT_COLUMN_STATS_TEMPLATE : TEXT_COLUMN_STATS_TEMPLATE)
            .add("column", columnName)
            .add("countColumn", PDAO_COUNT_COLUMN_NAME)
            .add("table", bqTableName(tdrResource, tableName))
            .add("tableName", tableName)
            .add("whereClause", whereClause)
            .add("direction", SqlSortDirection.ASC)
            .render();
    final TableResult result = bigQueryProject.query(bigQuerySQL);

    return (ColumnStatisticsTextModel)
        new ColumnStatisticsTextModel()
            .values(aggregateTextColumnStats(result, columnName))
            .dataType(column.getType().toString());
  }

  static List<ColumnStatisticsTextValue> aggregateTextColumnStats(
      TableResult result, String column) {
    List<ColumnStatisticsTextValue> values = new ArrayList<>();
    result
        .iterateAll()
        .forEach(
            rows -> {
              ColumnStatisticsTextValue val = new ColumnStatisticsTextValue();
              FieldValue fieldValue = rows.get(column);
              // getStringValue() throws NPE if value of field is null; getValue() does not
              Object rowValue = fieldValue.getValue();
              val.value(rowValue != null ? fieldValue.getStringValue() : null);
              val.count((int) (rows.get(PDAO_COUNT_COLUMN_NAME).getLongValue()));
              values.add(val);
            });
    return values;
  }

  public static ColumnStatisticsDoubleModel getStatsForDoubleColumn(
      FSContainerInterface tdrResource, String tableName, Column column, String filter)
      throws InterruptedException {

    final TableResult result = retrieveNumericColumnStats(tdrResource, tableName, column, filter);
    ColumnStatisticsDoubleModel doubleModel =
        (ColumnStatisticsDoubleModel)
            new ColumnStatisticsDoubleModel().dataType(column.getType().toString());
    setMinMaxDoubleResult(result, doubleModel);
    return doubleModel;
  }

  public static ColumnStatisticsIntModel getStatsForIntColumn(
      FSContainerInterface tdrResource, String tableName, Column column, String filter)
      throws InterruptedException {

    final TableResult result = retrieveNumericColumnStats(tdrResource, tableName, column, filter);
    ColumnStatisticsIntModel intModel =
        (ColumnStatisticsIntModel)
            new ColumnStatisticsIntModel().dataType(column.getType().toString());
    setMinMaxIntResult(result, intModel);
    return intModel;
  }

  private static TableResult retrieveNumericColumnStats(
      FSContainerInterface tdrResource, String tableName, Column column, String filter)
      throws InterruptedException {
    String whereClause = StringUtils.isNotEmpty(filter) ? filter : "";
    final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
    String columnName = column.getName();
    final String bigQuerySQL =
        new ST(
                column.isArrayOf()
                    ? ARRAY_NUMERIC_COLUMN_STATS_TEMPLATE
                    : NUMERIC_COLUMN_STATS_TEMPLATE)
            .add("column", columnName)
            .add("table", bqTableName(tdrResource, tableName))
            .add("whereClause", whereClause)
            .render();
    return bigQueryProject.query(bigQuerySQL);
  }

  private static void setMinMaxDoubleResult(
      TableResult tableResult, ColumnStatisticsDoubleModel doubleModel) {
    if (resultHasValue(tableResult, "min") && resultHasValue(tableResult, "max")) {
      doubleModel.minValue(getDoubleResult(tableResult, "min"));
      doubleModel.maxValue(getDoubleResult(tableResult, "max"));
    }
  }

  private static void setMinMaxIntResult(
      TableResult tableResult, ColumnStatisticsIntModel intModel) {
    if (resultHasValue(tableResult, "min") && resultHasValue(tableResult, "max")) {
      intModel.minValue(getIntResult(tableResult, "min"));
      intModel.maxValue(getIntResult(tableResult, "max"));
    }
  }

  // min and max fields will be undefined if the column only has null values or the table is empty
  private static boolean resultHasValue(TableResult tableResult, String statColumnName) {
    FieldValue fieldValue = tableResult.iterateAll().iterator().next().get(statColumnName);
    Object value = fieldValue.getValue();
    return value != null;
  }

  private static int getIntResult(TableResult tableResult, String statColumnName) {
    return (int) tableResult.iterateAll().iterator().next().get(statColumnName).getLongValue();
  }

  private static double getDoubleResult(TableResult tableResult, String statColumnName) {
    return tableResult.iterateAll().iterator().next().get(statColumnName).getDoubleValue();
  }
}
