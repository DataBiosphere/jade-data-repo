package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ANCESTOR_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.CHILD_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.DESCENDANT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ID_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.NUMCHILDREN_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PARENT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PATH_COLUMN_NAME;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.indexing.job.beam.PathUtils;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.azure.AzureRowResult;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.HierarchyMapping;
import bio.terra.tanagra.underlay.Underlay;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch Apache Beam pipeline for building a table that contains a path (i.e. a list of ancestors
 * in order) for each node in a hierarchy. Example row: (id,path)=(123,"456.789"), where 456 is the
 * parent of 123 and 789 is the grandparent of 123.
 */
public class BuildNumChildrenAndPaths extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(BuildNumChildrenAndPaths.class);
  private static final String TEMP_TABLE_SUFFIX = "pathNumChildren";

  // The default table schema for the id-path-numChildren output table.
  private static final TableSchema PATH_NUMCHILDREN_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              List.of(
                  new TableFieldSchema()
                      .setName(ID_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  // TODO: Consider how to handle other node types besides integer. One possibility
                  // is to serialize a list into a string. Another possibility is to make the path
                  // column an INTEGER/REPEATED field instead of a string, although that may only
                  // work for BQ and not other backends.
                  new TableFieldSchema()
                      .setName(PATH_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("NULLABLE"),
                  new TableFieldSchema()
                      .setName(NUMCHILDREN_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED")));

  private final String hierarchyName;

  public BuildNumChildrenAndPaths(Entity entity, String hierarchyName) {
    super(entity);
    this.hierarchyName = hierarchyName;
  }

  @Override
  public String getName() {
    return "BUILD CHILDREN COUNTS AND PATHS (" + getEntity().getName() + ", " + hierarchyName + ")";
  }

  @Override
  public void run(boolean isDryRun, Indexer.Executors executors) {
    // If the temp table hasn't been written yet, run the Dataflow job.
    if (!executors.index().checkTableExists(getTempTable())) {
      writeFieldsToTempTable(isDryRun, executors);
    } else {
      LOGGER.info("Temp table has already been written.");
    }

    // Dataflow jobs can only write new rows to BigQuery, so in this second step, copy over the
    // path/numChildren values to the corresponding columns in the entity table.
    copyFieldsToEntityTable(isDryRun, executors);
  }

  @Override
  public void clean(boolean isDryRun, Indexer.Executors executors) {
    executors.index().deleteTable(getTempTable(), isDryRun);
    // CreateEntityTable will delete the entity table, which includes all the rows updated by this
    // job.
  }

  @Override
  public JobStatus checkStatus(Indexer.Executors executors) {
    // Check if the temp table already exists.
    if (!executors.index().checkTableExists(getTempTable())) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the entity table already exists.
    if (!executors.index().checkTableExists(getEntityIndexTable())) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the table has at least 1 row where path IS NOT NULL.
    HierarchyMapping indexMapping =
        getEntity().getHierarchy(hierarchyName).getMapping(Underlay.MappingType.INDEX);
    ColumnSchema pathColumnSchema =
        getEntity()
            .getHierarchy(hierarchyName)
            .getField(HierarchyField.Type.PATH)
            .buildColumnSchema();
    return checkOneNotNullRowExists(indexMapping.getPathField(), pathColumnSchema, executors)
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }

  @VisibleForTesting
  public TablePointer getTempTable() {
    // Define a temporary table to write the id/path/num_children information to.
    // We can't write directly to the entity table because the Beam BigQuery library doesn't support
    // updating existing rows.
    return TablePointer.fromTableName(
        getTempTableName(hierarchyName + "_" + TEMP_TABLE_SUFFIX),
        getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer().getDataPointer());
  }

  private void writeFieldsToTempTable(boolean isDryRun, Indexer.Executors executors) {
    Query selectAllIds =
        getEntity().getMapping(Underlay.MappingType.SOURCE).queryIds(ID_COLUMN_NAME);
    LOGGER.info("select all ids SQL: {}", selectAllIds);

    // read in the nodes and the child-parent relationships from BQ
    List<Long> allNodes =
        executors.source().readTableRows(selectAllIds).stream()
            .map(rowResult -> rowResult.get(ID_COLUMN_NAME))
            .map(cellValue -> cellValue.getLong().orElseThrow())
            .toList();

    HierarchyMapping sourceHierarchyMapping =
        getEntity().getHierarchy(hierarchyName).getMapping(Underlay.MappingType.SOURCE);
    Query selectChildParentIdPairs =
        sourceHierarchyMapping.queryChildParentPairs(CHILD_COLUMN_NAME, PARENT_COLUMN_NAME);
    LOGGER.info("select all child-parent id pairs SQL: {}", selectChildParentIdPairs);

    List<Pair<Long, Long>> childParentRelationships =
        executors.source().readTableRows(selectChildParentIdPairs).stream()
            .map(
                rowResult ->
                    Pair.of(
                        rowResult.get(CHILD_COLUMN_NAME).getLong().orElseThrow(),
                        rowResult.get(PARENT_COLUMN_NAME).getLong().orElseThrow()))
            .toList();

    // compute a path to a root node for each node in the hierarchy
    Collection<Pair<Long, List<Long>>> nodePaths =
        PathUtils.computePaths(
            allNodes, childParentRelationships, sourceHierarchyMapping.getMaxHierarchyDepth());

    // count the number of children for each node in the hierarchy
    Map<Long, Long> nodeNumChildren = PathUtils.countChildren(childParentRelationships);

    // prune orphan nodes from the hierarchy (i.e. set path=null for nodes with no parents or
    // children)
    Collection<Pair<Long, List<Long>>> nodePrunedPathKVsPC =
        PathUtils.pruneOrphanPaths(nodePaths, nodeNumChildren);

    // filter the root nodes
    Collection<Pair<Long, List<Long>>> outputNodePath =
        filterRootNodes(sourceHierarchyMapping, executors, nodePrunedPathKVsPC);

    if (!isDryRun) {
      // create table if it doesn't exist
      writePathAndNumChildrenToIndex(
          outputNodePath, nodeNumChildren, getTempTable(), executors.index());
    }
  }

  /** Filter the root nodes, if a root nodes filter is specified by the hierarchy mapping. */
  private static Collection<Pair<Long, List<Long>>> filterRootNodes(
      HierarchyMapping sourceHierarchyMapping,
      Indexer.Executors executors,
      Collection<Pair<Long, List<Long>>> nodePrunedPath) {
    if (!sourceHierarchyMapping.hasRootNodesFilter()) {
      return nodePrunedPath;
    }
    Query selectPossibleRootIds = sourceHierarchyMapping.queryPossibleRootNodes(ID_COLUMN_NAME);
    LOGGER.info("select possible root ids SQL: {}", selectPossibleRootIds);

    // read in the possible root nodes from BQ
    Collection<Long> possibleRootNodes =
        executors.source().readTableRows(selectPossibleRootIds).stream()
            .map(rowResult -> rowResult.get(ID_COLUMN_NAME).getLong().orElseThrow())
            .toList();

    // filter the root nodes (i.e. set path=null for any existing root nodes that are not in the
    // list of possibles)
    return PathUtils.filterRootNodes(possibleRootNodes, nodePrunedPath);
  }

  static class IdPathChildrenRowResult implements RowResult {
    private final AzureRowResult row;

    public IdPathChildrenRowResult(long id, List<Long> path, long numChildren) {
      row =
          new AzureRowResult(
              Map.of(
                  ID_COLUMN_NAME,
                  Optional.of(id),
                  DESCENDANT_COLUMN_NAME,
                  Optional.of(path.stream().map(String::valueOf).collect(Collectors.joining("."))),
                  NUMCHILDREN_COLUMN_NAME,
                  Optional.of(numChildren)),
              new ColumnHeaderSchema(
                  List.of(
                      new ColumnSchema(ID_COLUMN_NAME, CellValue.SQLDataType.INT64),
                      new ColumnSchema(DESCENDANT_COLUMN_NAME, CellValue.SQLDataType.STRING),
                      new ColumnSchema(NUMCHILDREN_COLUMN_NAME, CellValue.SQLDataType.INT64))));
    }

    @Override
    public CellValue get(int index) {
      return row.get(index);
    }

    @Override
    public CellValue get(String columnName) {
      return row.get(columnName);
    }

    @Override
    public int size() {
      return row.size();
    }
  }

  /** Write the {@link KV} pairs (id, path, num_children) to BQ. */
  private static void writePathAndNumChildrenToIndex(
      Collection<Pair<Long, List<Long>>> nodePaths,
      Map<Long, Long> nodeNumChildren,
      TablePointer outputTable,
      QueryExecutor executor) {
    TableVariable forPrimary = TableVariable.forPrimary(outputTable);
    Map<String, FieldVariable> updateFields =
        Map.of(
            ID_COLUMN_NAME,
            new FieldVariable(
                new FieldPointer.Builder().columnName(ANCESTOR_COLUMN_NAME).build(), forPrimary),
            PATH_COLUMN_NAME,
            new FieldVariable(
                new FieldPointer.Builder().columnName(DESCENDANT_COLUMN_NAME).build(), forPrimary),
            NUMCHILDREN_COLUMN_NAME,
            new FieldVariable(
                new FieldPointer.Builder().columnName(NUMCHILDREN_COLUMN_NAME).build(),
                forPrimary));
    //
    //    SQLExpression insertQuery =
    //        new UpdateFromValues(
    //            forPrimary,
    //            updateFields,
    //            updateFields.get(ID_COLUMN_NAME),
    //
    //            nodePaths.stream().map(nodePath -> (RowResult) new
    // IdPathChildrenRowResult(nodePath.getKey(), nodePath.getValue(),
    // nodeNumChildren.get(nodePath.getKey()))).toList());
    //
    //    insertUpdateTableFromSelect()
    //    idPathAndNumChildrenBQRows.apply(
    //        "insert the (id, path, numChildren) rows into BQ",
    //        BigQueryIO.writeTableRows()
    //            .to(outputBQTable.getPathForIndexing())
    //            .withSchema(PATH_NUMCHILDREN_TABLE_SCHEMA)
    //            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
    //            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
    //            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
  }

  private void copyFieldsToEntityTable(boolean isDryRun, Indexer.Executors executors) {
    // Copy the fields to the entity table.
    Hierarchy hierarchy = getEntity().getHierarchy(hierarchyName);
    HierarchyMapping indexMapping = hierarchy.getMapping(Underlay.MappingType.INDEX);

    // Build a query for the id-path-num_children tuples that we want to select.
    Query idPathNumChildrenTuples = HierarchyMapping.queryPathNumChildrenPairs(getTempTable());

    // Build a map of (output) update field name -> (input) selected FieldVariable.
    // This map only contains two items, because we're only updating the path and numChildren
    // fields.
    Map<String, FieldVariable> updateFields = new HashMap<>();
    String updatePathFieldName = indexMapping.getPathField().getColumnName();
    FieldVariable selectPathField =
        idPathNumChildrenTuples.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(PATH_COLUMN_NAME))
            .findFirst()
            .get();
    updateFields.put(updatePathFieldName, selectPathField);

    String updateNumChildrenFieldName = indexMapping.getNumChildrenField().getColumnName();
    FieldVariable selectNumChildrenField =
        idPathNumChildrenTuples.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(NUMCHILDREN_COLUMN_NAME))
            .findFirst()
            .get();
    updateFields.put(updateNumChildrenFieldName, selectNumChildrenField);

    // Check that the path field is not in a different table from the entity table.
    if (indexMapping.getPathField().isForeignKey()
        || !indexMapping
            .getPathField()
            .getTablePointer()
            .equals(getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer())) {
      throw new SystemException(
          "Indexing path, num_children information only supports an index mapping to a column in the entity table");
    }

    updateEntityTableFromSelect(
        idPathNumChildrenTuples, updateFields, ID_COLUMN_NAME, isDryRun, executors);
  }
}
