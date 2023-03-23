package bio.terra.tanagra.indexing.job;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.Underlay;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRelationshipIdPairs extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteRelationshipIdPairs.class);

  private final Relationship relationship;

  public WriteRelationshipIdPairs(Relationship relationship) {
    super(relationship.getEntityA());
    this.relationship = relationship;
  }

  @Override
  public String getName() {
    return "WRITE RELATIONSHIP ID PAIRS ("
        + relationship.getEntityA().getName()
        + ", "
        + relationship.getEntityB().getName()
        + ")";
  }

  @Override
  public void run(boolean isDryRun, QueryExecutor executor) {
    String idAAlias =
        relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsIdA().getColumnName();
    String idBAlias =
        relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsIdB().getColumnName();
    SQLExpression selectRelationshipIdPairs =
        relationship.getMapping(Underlay.MappingType.SOURCE).queryIdPairs(idAAlias, idBAlias);
    String sql = selectRelationshipIdPairs.renderSQL();
    LOGGER.info("select all relationship id pairs SQL: {}", sql);

    TableId destinationTable =
        TableId.of(
            getBQDataPointer(getAuxiliaryTable()).getProjectId(),
            getBQDataPointer(getAuxiliaryTable()).getDatasetId(),
            getAuxiliaryTable().getTableName());
    executor.createTableFromQuery(destinationTable, sql, isDryRun);
  }

  @Override
  public void clean(boolean isDryRun, QueryExecutor executor) {
    if (checkTableExists(getAuxiliaryTable(), executor)) {
      deleteTable(getAuxiliaryTable(), isDryRun);
    }
  }

  @Override
  public JobStatus checkStatus(QueryExecutor executor) {
    // Check if the table already exists.
    return checkTableExists(getAuxiliaryTable(), executor)
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }

  public TablePointer getAuxiliaryTable() {
    return relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsTable();
  }
}
