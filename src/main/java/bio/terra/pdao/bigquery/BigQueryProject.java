package bio.terra.pdao.bigquery;

import bio.terra.pdao.exception.PdaoException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BigQueryProject {
    private final String projectId;
    private final BigQuery bigQuery;

    public BigQueryProject(String projectId) {
        this.projectId = projectId;
        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    public BigQueryProject(String projectId, GoogleCredentials googleCredentials){
        this.projectId = projectId;
        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(googleCredentials)
            .build()
            .getService();
    }

    public String getProjectId() {
        return projectId;
    }

    public BigQuery getBigQuery() {
        return bigQuery;
    }

    public boolean datasetExists(String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            return (dataset != null);
        } catch (Exception ex) {
            throw new PdaoException("existence check failed for " + datasetName, ex);
        }
    }

    public DatasetId createDataset(String name, String description) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(name)
            .setDescription(description)
            .build();
        return bigQuery.create(datasetInfo).getDatasetId();
    }

    public boolean deleteDataset(String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            return bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
        } catch (Exception ex) {
            throw new PdaoException("delete failed for " + datasetName, ex);
        }
    }

    public void createTable(String datasetName, String tableName, Schema schema) {
        TableId tableId = TableId.of(datasetName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }

    public boolean deleteTable(String datasetName, String tableName) {
        TableId tableId = TableId.of(projectId, datasetName, tableName);
        return bigQuery.delete(tableId);
    }

    public void updateDatasetAcls(Dataset dataset, List<Acl> acls) {
        DatasetInfo datasetInfo = dataset.toBuilder().setAcl(acls).build();
        bigQuery.update(datasetInfo);
    }

    public void addDatasetAcls(String datasetId, List<Acl> acls) {
        Dataset dataset = bigQuery.getDataset(datasetId);
        List<Acl> beforeAcls = dataset.getAcl();
        ArrayList<Acl> newAcls = new ArrayList<>(beforeAcls);
        newAcls.addAll(acls);
        updateDatasetAcls(dataset, newAcls);
    }

    public void removeDatasetAcls(String datasetId, List<Acl> acls) {
        Dataset dataset = bigQuery.getDataset(datasetId);
        if (dataset != null) {  // can be null if create dataset step failed before it was created
            Set<Acl> datasetAcls = new HashSet(dataset.getAcl());
            datasetAcls.removeAll(acls);
            updateDatasetAcls(dataset, new ArrayList(datasetAcls));
        }
    }

    public TableResult query(String sql) {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            return bigQuery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Query unexpectedly interrupted", e);
        } catch (BigQueryException e) {
            throw new PdaoException("Failure executing query", e);
        }
    }
}
