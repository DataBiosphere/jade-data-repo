package bio.terra.integration;

import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotModel;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class BigQueryFixtures {
    private static final int SAM_TIMEOUT_SECONDS = 400;

    private BigQueryFixtures() {
    }

    public static BigQuery getBigQuery(String projectId, Credentials credentials) {
        return BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();
    }

    public static BigQuery getBigQuery(String projectId, String token) {
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        return getBigQuery(projectId, googleCredentials);
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
            e.printStackTrace(System.out);
            throw new IllegalStateException("Query failed", e);
        }
    }

    public static String makeTableRef(SnapshotModel snapshotModel, String tableName) {
        return String.format("`%s.%s.%s`",
            snapshotModel.getDataProject(),
            snapshotModel.getName(),
            tableName);
    }

    public static String makeTableRef(DatasetModel datasetModel, String tableName) {
        return String.format("`%s.%s.%s`",
            datasetModel.getDataProject(),
            PdaoConstant.PDAO_PREFIX + datasetModel.getName(),
            tableName);
    }

    // Given a dataset, table, and column, query for a DRS URI and extract the DRS Object Id
    private static final Pattern drsIdRegex = Pattern.compile("([^/]+)$");

    public static String queryForDrsId(BigQuery bigQuery,
                                       SnapshotModel snapshotModel,
                                       String tableName,
                                       String columnName) {
        String sql = String.format("SELECT %s FROM %s WHERE %s IS NOT NULL LIMIT 1",
            columnName,
            makeTableRef(snapshotModel, tableName),
            columnName);
        TableResult ids = BigQueryFixtures.query(sql, bigQuery);
        assertThat("Got one row", ids.getTotalRows(), equalTo(1L));

        String drsUri = null;
        for (FieldValueList fieldValueList : ids.iterateAll()) {
            drsUri = fieldValueList.get(0).getStringValue();
        }
        assertThat("DRS URI was found", drsUri, notNullValue());

        Matcher matcher = drsIdRegex.matcher(drsUri);
        assertThat("matcher found a match in the DRS URI", matcher.find(), equalTo(true));
        return matcher.group();
    }

    // Common method to use to wait for SAM to sync to a google group, allowing access by the
    // user associated with the BigQuery instance.
    public static boolean hasAccess(BigQuery bigQuery, String dataProject, String bqDatasetName) throws Exception {
        return TestUtils.eventualExpect(5, SAM_TIMEOUT_SECONDS, true, () -> {
            try {
                boolean bqDatasetExists = BigQueryFixtures.datasetExists(bigQuery, dataProject, bqDatasetName);
                assertTrue("BigQuery dataset exists and is accessible", bqDatasetExists);
                return true;
            } catch (IllegalStateException e) {
                assertThat(
                    "access is denied until SAM syncs the reader policy with Google",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
                return false;
            }
        });
    }

}
