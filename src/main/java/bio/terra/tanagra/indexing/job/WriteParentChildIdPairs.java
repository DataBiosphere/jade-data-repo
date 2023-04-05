package bio.terra.tanagra.indexing.job;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Underlay;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteParentChildIdPairs extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteParentChildIdPairs.class);

  private final String hierarchyName;

  public WriteParentChildIdPairs(Entity entity, String hierarchyName) {
    super(entity);
    this.hierarchyName = hierarchyName;
  }

  @Override
  public String getName() {
    return "WRITE PARENT-CHILD ID PAIRS (" + getEntity().getName() + ", " + hierarchyName + ")";
  }

  @Override
  public void run(boolean isDryRun, Indexer.Executors executors) {
    SQLExpression selectChildParentIdPairs =
        getEntity()
            .getHierarchy(hierarchyName)
            .getMapping(Underlay.MappingType.SOURCE)
            .queryChildParentPairs("child", "parent");
    String sql = executors.source().renderSQL(selectChildParentIdPairs);
    LOGGER.info("select all child-parent id pairs SQL: {}", sql);

    TableId destinationTable =
        TableId.of(
            getBQDataPointer(getAuxiliaryTable()).getProjectId(),
            getBQDataPointer(getAuxiliaryTable()).getDatasetId(),
            getAuxiliaryTable().getTableName());
    getBQDataPointer(getAuxiliaryTable())
        .getBigQueryService()
        .createTableFromQuery(destinationTable, sql, isDryRun);
  }

  @Override
  public void clean(boolean isDryRun, Indexer.Executors executors) {
    if (checkTableExists(getAuxiliaryTable(), executors.index())) {
      deleteTable(getAuxiliaryTable(), isDryRun, executors.index());
    }
  }

  @Override
  public JobStatus checkStatus(Indexer.Executors executors) {
    // Check if the table already exists.
    return checkTableExists(getAuxiliaryTable(), executors.index())
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }

  public TablePointer getAuxiliaryTable() {
    return getEntity()
        .getHierarchy(hierarchyName)
        .getMapping(Underlay.MappingType.INDEX)
        .getChildParent()
        .getTablePointer();
  }
}
