package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ANCESTOR_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.CHILD_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.DESCENDANT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PARENT_COLUMN_NAME;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.InsertFromValues;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.HierarchyMapping;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.datapointer.BigQueryDataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import java.util.Map;

/**
 * A batch Apache Beam pipeline for flattening hierarchical parent-child relationships to
 * ancestor-descendant relationships.
 */
public class WriteAncestorDescendantIdPairs extends BigQueryIndexingJob {
  // The default table schema for the ancestor-descendant output table.
  private static final Schema ANCESTOR_DESCENDANT_TABLE_SCHEMA =
      Schema.of(
          Field.newBuilder(ANCESTOR_COLUMN_NAME, StandardSQLTypeName.INT64)
              .setMode(Field.Mode.REQUIRED)
              .build(),
          Field.newBuilder(DESCENDANT_COLUMN_NAME, StandardSQLTypeName.INT64)
              .setMode(Field.Mode.REQUIRED)
              .build());

  private final String hierarchyName;

  public WriteAncestorDescendantIdPairs(Entity entity, String hierarchyName) {
    super(entity);
    this.hierarchyName = hierarchyName;
  }

  @Override
  public String getName() {
    return "WRITE ANCESTOR-DESCENDANT ID PAIRS ("
        + getEntity().getName()
        + ", "
        + hierarchyName
        + ")";
  }

  @Override
  public void run(boolean isDryRun, Indexer.Executors executors) {

    // Read hierarchy pairs.
    HierarchyMapping sourceHierarchyMapping =
        getEntity().getHierarchy(hierarchyName).getMapping(Underlay.MappingType.SOURCE);
    Query selectChildParentIdPairs =
        sourceHierarchyMapping.queryChildParentPairs(CHILD_COLUMN_NAME, PARENT_COLUMN_NAME);
    var relationships = executors.source().readTableRows(selectChildParentIdPairs);

    // Create pairs table.
    BigQueryDataset indexDataset = getBQDataPointer(getAuxiliaryTable());
    TableId auxTableId =
        TableId.of(
            indexDataset.getProjectId(),
            indexDataset.getDatasetId(),
            getAuxiliaryTable().getTableName());
    indexDataset
        .getBigQueryService()
        .createTableFromSchema(auxTableId, ANCESTOR_DESCENDANT_TABLE_SCHEMA, isDryRun);

    // Write pairs to table.
    TableVariable outputTable = TableVariable.forPrimary(getAuxiliaryTable());
    SQLExpression insertQuery =
        new InsertFromValues(
            outputTable,
            Map.of(
                PARENT_COLUMN_NAME,
                new FieldVariable(
                    new FieldPointer.Builder().columnName(ANCESTOR_COLUMN_NAME).build(),
                    outputTable),
                CHILD_COLUMN_NAME,
                new FieldVariable(
                    new FieldPointer.Builder().columnName(DESCENDANT_COLUMN_NAME).build(),
                    outputTable)),
            relationships);
    insertUpdateTableFromSelect(executors.index().renderSQL(insertQuery), isDryRun);
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
        .getAncestorDescendant()
        .getTablePointer();
  }
}
