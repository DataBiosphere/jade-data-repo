package bio.terra.service.tabulardata.google.bigquery;

import static bio.terra.common.PdaoConstant.*;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
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

    ST sqlTemplate = new ST(selectHasDuplicateStagingIdsTemplate);
    sqlTemplate.add("count", PDAO_COUNT_ALIAS);
    sqlTemplate.add("project", bigQueryProject.getProjectId());
    sqlTemplate.add("dataset", bqDatasetName);
    sqlTemplate.add("tableName", tableName);
    sqlTemplate.add("pkColumns", pkColumns);

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
}
