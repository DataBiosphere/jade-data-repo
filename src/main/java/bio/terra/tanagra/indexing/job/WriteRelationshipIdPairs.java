package bio.terra.tanagra.indexing.job;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.Indexer;
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
  public void run(boolean isDryRun, Indexer.Executors executors) {
    String idAAlias =
        relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsIdA().getColumnName();
    String idBAlias =
        relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsIdB().getColumnName();
    SQLExpression selectRelationshipIdPairs =
        relationship.getMapping(Underlay.MappingType.SOURCE).queryIdPairs(idAAlias, idBAlias);
    String sql = executors.source().renderSQL(selectRelationshipIdPairs);
    LOGGER.info("select all relationship id pairs SQL: {}", sql);

    TableId destinationTable =
        TableId.of(
            getBQDataPointer(getAuxiliaryTable()).getProjectId(),
            getBQDataPointer(getAuxiliaryTable()).getDatasetId(),
            getAuxiliaryTable().getTableName());
    executors.index().createTableFromQuery(destinationTable, sql, isDryRun);
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
    return relationship.getMapping(Underlay.MappingType.INDEX).getIdPairsTable();
  }
}
