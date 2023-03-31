package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.PDAO_COUNT_ALIAS;
import static bio.terra.common.PdaoConstant.PDAO_EXTERNAL_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_PREFIX;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.exception.PdaoException;
import bio.terra.grammar.Query;
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
import org.stringtemplate.v4.ST;

public abstract class BigQueryPdao {

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
  public static final String DATA_TEMPLATE = "SELECT <columns> FROM <table> <filterParams>";

  public static final String DATA_FILTER_TEMPLATE =
      "<whereClause> ORDER BY <sort> <direction> LIMIT <limit> OFFSET <offset>";

  /*
   * WARNING: Ensure input parameters are validated before executing this method!
   */
  public static List<Map<String, Object>> getTable(
      FSContainerInterface tdrResource,
      String bqFormattedTableName,
      List<String> columnNames,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter)
      throws InterruptedException {
    final BigQueryProject bigQueryProject = BigQueryProject.from(tdrResource);
    final String datasetProjectId = bigQueryProject.getProjectId();
    String whereClause = StringUtils.isNotEmpty(filter) ? filter : "";

    String columns = String.join(",", columnNames);
    // Parse before querying because the where clause is user-provided
    // TODO - This code should be shared with Azure equivalent call (DR-2937)
    final String sql =
        new ST(DATA_TEMPLATE)
            .add("columns", columns)
            .add("table", bqFormattedTableName)
            .add("filterParams", whereClause)
            .render();
    Query.parse(sql);

    // The bigquery sql table name must be enclosed in backticks
    String bigQueryTable = "`" + datasetProjectId + "." + bqFormattedTableName + "`";
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
            .add("table", bigQueryTable)
            .add("filterParams", filterParams)
            .render();
    final TableResult result = bigQueryProject.query(bigQuerySQL);
    return aggregateTableData(result);
  }

  public static List<Map<String, Object>> aggregateTableData(TableResult result) {
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
}
