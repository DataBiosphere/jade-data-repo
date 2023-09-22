package bio.terra.tanagra.query;

import bio.terra.service.dataset.DatasetTable;
import com.google.cloud.bigquery.TableId;
import java.util.Collection;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface QueryExecutor {
  Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

  /** Execute a query request, returning the results of the query. */
  QueryResult execute(QueryRequest queryRequest);

  void createTableFromQuery(TableId destinationTable, String sql, boolean isDryRun);

  Collection<RowResult> readTableRows(Query query);

  SqlPlatform getSqlPlatform();

  default String renderSQL(SQLExpression expression) {
    String sql = expression.renderSQL(getSqlPlatform());
    LOGGER.info("Generated SQL: {}", sql);
    return sql;
  }

  DatasetTable getSchema(UUID datasetId, String tableName);

  default void deleteTable(TablePointer tablePointer, boolean isDryRun) {
    throw new NotImplementedException();
  }

  // -----Helper methods for checking whether a job has run already.-------
  default boolean checkTableExists(TablePointer tablePointer) {
    throw new NotImplementedException();
  }
}
