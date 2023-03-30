package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import com.google.cloud.bigquery.TableId;
import java.util.Collection;

public interface QueryExecutor {
  /** Execute a query request, returning the results of the query. */
  QueryResult execute(QueryRequest queryRequest);

  void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun);

  Collection<RowResult> readTableRows(Query query);

  CloudPlatform getCloudPlatform();

  default String renderSQL(SQLExpression expression) {
    return expression.renderSQL(getCloudPlatform());
  }
}
