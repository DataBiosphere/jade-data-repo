package utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;


public final class BigQueryUtils {

  private BigQueryUtils() {}

    /**
   * Executes a command in a separate process.
   *
   * @param dataproject the name of the data project
   * @param query the query to run on BQ
   * @return the result of the BQ query
   * @throws InterruptedException from the bigQuery.query() method
   */
  public static TableResult queryBigQuery(String dataproject, String query) throws InterruptedException {
      // build the BQ object
      BigQuery bigQuery =
          BigQueryOptions.newBuilder()
              .setProjectId(dataproject)
              .build()
              .getService();

      // query the BQ object
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
      return bigQuery.query(queryConfig);
  }
}
