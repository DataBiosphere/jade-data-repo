package bio.terra.integration;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public final class BigQueryFixtures {
    private BigQueryFixtures() {
    }

    public static BigQuery getBigQuery(String projectId, Credentials credentials) {
        return BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();
    }

    public static boolean datasetExists(BigQuery bigQuery, String projectId, String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            return (dataset != null);
        } catch (Exception ex) {
            throw new IllegalStateException("existence check failed for " + datasetName, ex);
        }
    }

    public static TableResult query(String sql, BigQuery bigQuery) {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            return bigQuery.query(queryConfig);
        } catch (InterruptedException | BigQueryException e) {
            throw new IllegalStateException("Query failed", e);
        }
    }
}
