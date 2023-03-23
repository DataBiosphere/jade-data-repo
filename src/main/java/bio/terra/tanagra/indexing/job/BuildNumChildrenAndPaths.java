package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.CHILD_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ID_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.NUMCHILDREN_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PARENT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PATH_COLUMN_NAME;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.job.beam.BigQueryUtils;
import bio.terra.tanagra.indexing.job.beam.PathUtils;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.HierarchyMapping;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.datapointer.BigQueryDataset;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
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
  public void run(boolean isDryRun, QueryExecutor executor) {
    // If the temp table hasn't been written yet, run the Dataflow job.
    if (!checkTableExists(getTempTable(), executor)) {
      writeFieldsToTempTable(isDryRun);
    } else {
      LOGGER.info("Temp table has already been written.");
    }

    // Dataflow jobs can only write new rows to BigQuery, so in this second step, copy over the
    // path/numChildren values to the corresponding columns in the entity table.
    copyFieldsToEntityTable(isDryRun, executor);
  }

  @Override
  public void clean(boolean isDryRun, QueryExecutor executor) {
    if (checkTableExists(getTempTable(), executor)) {
      deleteTable(getTempTable(), isDryRun);
    }
    // CreateEntityTable will delete the entity table, which includes all the rows updated by this
    // job.
  }

  @Override
  public JobStatus checkStatus(QueryExecutor executor) {
    // Check if the temp table already exists.
    if (!checkTableExists(getTempTable(), executor)) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the entity table already exists.
    if (!checkTableExists(getEntityIndexTable(), executor)) {
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
    return checkOneNotNullRowExists(indexMapping.getPathField(), pathColumnSchema, executor)
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

  private void writeFieldsToTempTable(boolean isDryRun) {
    String selectAllIdsSql =
        getEntity().getMapping(Underlay.MappingType.SOURCE).queryIds(ID_COLUMN_NAME).renderSQL();
    LOGGER.info("select all ids SQL: {}", selectAllIdsSql);

    HierarchyMapping sourceHierarchyMapping =
        getEntity().getHierarchy(hierarchyName).getMapping(Underlay.MappingType.SOURCE);
    String selectChildParentIdPairsSql =
        sourceHierarchyMapping
            .queryChildParentPairs(CHILD_COLUMN_NAME, PARENT_COLUMN_NAME)
            .renderSQL();
    LOGGER.info("select all child-parent id pairs SQL: {}", selectChildParentIdPairsSql);

    BigQueryDataset outputBQDataset = getBQDataPointer(getTempTable());
    Pipeline pipeline = Pipeline.create(buildDataflowPipelineOptions(outputBQDataset));

    // read in the nodes and the child-parent relationships from BQ
    PCollection<Long> allNodesPC =
        BigQueryUtils.readNodesFromBQ(pipeline, selectAllIdsSql, "allNodes");
    PCollection<KV<Long, Long>> childParentRelationshipsPC =
        BigQueryUtils.readChildParentRelationshipsFromBQ(pipeline, selectChildParentIdPairsSql);

    // compute a path to a root node for each node in the hierarchy
    PCollection<KV<Long, String>> nodePathKVsPC =
        PathUtils.computePaths(
            allNodesPC, childParentRelationshipsPC, sourceHierarchyMapping.getMaxHierarchyDepth());

    // count the number of children for each node in the hierarchy
    PCollection<KV<Long, Long>> nodeNumChildrenKVsPC =
        PathUtils.countChildren(allNodesPC, childParentRelationshipsPC);

    // prune orphan nodes from the hierarchy (i.e. set path=null for nodes with no parents or
    // children)
    PCollection<KV<Long, String>> nodePrunedPathKVsPC =
        PathUtils.pruneOrphanPaths(nodePathKVsPC, nodeNumChildrenKVsPC);

    // filter the root nodes
    PCollection<KV<Long, String>> outputNodePathKVsPC =
        filterRootNodes(sourceHierarchyMapping, pipeline, nodePrunedPathKVsPC);

    // write the node-{path, numChildren} pairs to BQ
    writePathAndNumChildrenToBQ(outputNodePathKVsPC, nodeNumChildrenKVsPC, getTempTable());

    if (!isDryRun) {
      pipeline.run().waitUntilFinish();
    }
  }

  /** Filter the root nodes, if a root nodes filter is specified by the hierarchy mapping. */
  private static PCollection<KV<Long, String>> filterRootNodes(
      HierarchyMapping sourceHierarchyMapping,
      Pipeline pipeline,
      PCollection<KV<Long, String>> nodePrunedPathKVsPC) {
    if (!sourceHierarchyMapping.hasRootNodesFilter()) {
      return nodePrunedPathKVsPC;
    }
    String selectPossibleRootIdsSql =
        sourceHierarchyMapping.queryPossibleRootNodes(ID_COLUMN_NAME).renderSQL();
    LOGGER.info("select possible root ids SQL: {}", selectPossibleRootIdsSql);

    // read in the possible root nodes from BQ
    PCollection<Long> possibleRootNodesPC =
        BigQueryUtils.readNodesFromBQ(pipeline, selectPossibleRootIdsSql, "rootNodes");

    // filter the root nodes (i.e. set path=null for any existing root nodes that are not in the
    // list of possibles)
    return PathUtils.filterRootNodes(possibleRootNodesPC, nodePrunedPathKVsPC);
  }

  /** Write the {@link KV} pairs (id, path, num_children) to BQ. */
  private static void writePathAndNumChildrenToBQ(
      PCollection<KV<Long, String>> nodePathKVs,
      PCollection<KV<Long, Long>> nodeNumChildrenKVs,
      TablePointer outputBQTable) {
    // define the CoGroupByKey tags
    final TupleTag<String> pathTag = new TupleTag<>();
    final TupleTag<Long> numChildrenTag = new TupleTag<>();

    // do a CoGroupByKey join of the current id-numChildren collection and the parent-child
    // collection
    PCollection<KV<Long, CoGbkResult>> pathNumChildrenJoin =
        KeyedPCollectionTuple.of(pathTag, nodePathKVs)
            .and(numChildrenTag, nodeNumChildrenKVs)
            .apply(
                "join id-path and id-numChildren collections for BQ row generation",
                CoGroupByKey.create());

    // run a ParDo for each row of the join result
    PCollection<TableRow> idPathAndNumChildrenBQRows =
        pathNumChildrenJoin.apply(
            "run ParDo for each row of the id-path and id-numChildren join result to build the BQ (id, path, numChildren) row objects",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, TableRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, CoGbkResult> element = context.element();
                    Long node = element.getKey();
                    Iterator<String> pathTagIter = element.getValue().getAll(pathTag).iterator();
                    Iterator<Long> numChildrenTagIter =
                        element.getValue().getAll(numChildrenTag).iterator();

                    String path = pathTagIter.next();
                    Long numChildren = numChildrenTagIter.next();

                    context.output(
                        new TableRow()
                            .set(ID_COLUMN_NAME, node)
                            .set(PATH_COLUMN_NAME, path)
                            .set(NUMCHILDREN_COLUMN_NAME, numChildren));
                  }
                }));

    idPathAndNumChildrenBQRows.apply(
        "insert the (id, path, numChildren) rows into BQ",
        BigQueryIO.writeTableRows()
            .to(outputBQTable.getPathForIndexing())
            .withSchema(PATH_NUMCHILDREN_TABLE_SCHEMA)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
  }

  private void copyFieldsToEntityTable(boolean isDryRun, QueryExecutor executor) {
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
        idPathNumChildrenTuples, updateFields, ID_COLUMN_NAME, isDryRun, executor);
  }
}
