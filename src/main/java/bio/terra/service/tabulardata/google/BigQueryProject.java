package bio.terra.service.tabulardata.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.dataset.BigQueryPartitionConfigV1;
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
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class BigQueryProject {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryProject.class);
    private static final ConcurrentHashMap<String, BigQueryProject> bigQueryProjectCache = new ConcurrentHashMap<>();
    private final String projectId;
    private final BigQuery bigQuery;

    private BigQueryProject(String projectId) {
        logger.info("Retrieving Bigquery project for project id: {}", projectId);
        this.projectId = projectId;
        bigQuery = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    public static BigQueryProject get(String projectId) {
        bigQueryProjectCache.computeIfAbsent(projectId, BigQueryProject::new);
        return bigQueryProjectCache.get(projectId);
    }

    public String getProjectId() {
        return projectId;
    }

    public BigQuery getBigQuery() {
        return bigQuery;
    }

    // TODO: REVIEWERS PLEASE CHECK: Should these methods be in here? On the one hand, it is convenient. But it
    // mixes the duties of this class: it is supplying both the cache and the BQ methods. Unfortunately, it
    // doesn't supply all of the BQ methods, so sometimes getBigQuery is needed. Seems like it could be
    // improved.
    //
    // In general, BigQueryPdao is too big and needs refactoring. Perhaps when that is done, we can also
    // re-think this code structure.

    public boolean datasetExists(String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            return (dataset != null);
        } catch (Exception ex) {
            throw new PdaoException("existence check failed for " + datasetName, ex);
        }
    }

    public boolean tableExists(String datasetName, String tableName) {
        try {
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            Table table = bigQuery.getTable(tableId);
            return (table != null);
        } catch (Exception ex) {
            throw new PdaoException("existence check failed for " + datasetName + "." + tableName, ex);
        }
    }

    public DatasetId createDataset(String name, String description, GoogleRegion region) {
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(name)
            .setDescription(description)
            .setLocation(region.toString())
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
        createTable(datasetName, tableName, schema, BigQueryPartitionConfigV1.none());
    }

    public void createTable(String datasetName, String tableName, Schema schema,
                            BigQueryPartitionConfigV1 partitionConfig) {
        TableId tableId = TableId.of(datasetName, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(partitionConfig.asTimePartitioning())
            .setRangePartitioning(partitionConfig.asRangePartitioning())
            .build();
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
        logger.info("ADD DATASET ACLS");
        logger.info("dataset id " + datasetId);

        Dataset dataset = bigQuery.getDataset(datasetId);
        logger.info(dataset.toString());

        List<Acl> beforeAcls = dataset.getAcl();
        logger.debug("Before acl: " + StringUtils.join(beforeAcls, ", "));
        ArrayList<Acl> newAcls = new ArrayList<>(beforeAcls);
        newAcls.addAll(acls);
        logger.debug("New acl: " + StringUtils.join(newAcls, ", "));
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

    public TableResult query(String sql) throws InterruptedException {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
            return bigQuery.query(queryConfig);
        } catch (BigQueryException e) {
            throw new PdaoException("Failure executing query...\n" + sql, e);
        }
    }
}
