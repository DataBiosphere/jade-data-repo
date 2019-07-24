package bio.terra.pdao.bigquery;

import bio.terra.filesystem.ProjectAndCredential;
import bio.terra.pdao.exception.PdaoException;
import com.google.auth.Credentials;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BigQueryProject {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryProject.class);
    private static HashMap<ProjectAndCredential, BigQueryProject> bigQueryProjectLookup = new HashMap<>();
    private final String projectId;
    private final BigQuery bigQuery;

    public BigQueryProject(String projectId) {
        logger.info("Retrieving Bigquery project for project id: {}", projectId);
        this.projectId = projectId;
        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    public BigQueryProject(String projectId, Credentials credentials) {
        logger.info("Retrieving Bigquery project for project id: {}", projectId);
        this.projectId = projectId;
        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();
    }

    public static BigQueryProject get(String projectId, Credentials credentials) {
        ProjectAndCredential projectAndCredential = new ProjectAndCredential(projectId, credentials);
        if (!bigQueryProjectLookup.containsKey(projectAndCredential)) {
            BigQueryProject bigQueryProject;
            if (credentials == null) {
                bigQueryProject = new BigQueryProject(projectId);
            } else {
                bigQueryProject = new BigQueryProject(projectId, credentials);
            }
            bigQueryProjectLookup.put(projectAndCredential, bigQueryProject);
        }
        return bigQueryProjectLookup.get(projectAndCredential);
    }

    public static BigQueryProject get(String projectId) {
        return get(projectId, null);
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
