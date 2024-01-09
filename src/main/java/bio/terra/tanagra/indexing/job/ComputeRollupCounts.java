package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ANCESTOR_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.COUNT_ID_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.DESCENDANT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ID_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ROLLUP_COUNT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ROLLUP_DISPLAY_HINTS_COLUMN_NAME;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.Indexer;
import bio.terra.tanagra.indexing.job.beam.BigQueryUtils;
import bio.terra.tanagra.indexing.job.beam.CountUtils;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.RelationshipMapping;
import bio.terra.tanagra.underlay.Underlay;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Count the number of distinct occurrences for entity, which may optionally include a hierarchy,
 * and writes the results to the index entity BQ table.
 *
 * <p>This job is called 4 times for condition_person_occurrence entity group. For SDD condition
 * 22274:
 *
 * <pre>
 * criteriaToPrimary relationship, no hierarchy:
 *
 *     t_count_person column = 755 will be added to index condition table, because 775 people have
 *          a condition occurrence entity with condition 22274
 *
 * criteriaToPrimary relationship, standard hierarchy:
 *
 *     t_count_person_standard: 775 people have an occurrence entity with condition 22274 or a
 *         condition below it in the hierarchy
 *
 * criteriaToOccurrence, no hierarchy:
 *
 *   t_count_condition_occurrence: 3379 occurrences (across all people) for condition 22274
 *
 * criteriaToOccurrence, standard hierarchy:
 *
 *   t_count_condition_occurrence_standard: 3380 occurrences (across all people) for condition 22274
 *       or conditions below 22274 in the hierarchy
 * </pre>
 */
public class ComputeRollupCounts extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComputeRollupCounts.class);

  private static final String TEMP_TABLE_PREFIX = "rollup_";

  // The default table schema for the id-rollupCount-rollupDisplayHints output table.
  private static final TableSchema ROLLUP_COUNT_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              List.of(
                  new TableFieldSchema()
                      .setName(ID_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ROLLUP_COUNT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(ROLLUP_DISPLAY_HINTS_COLUMN_NAME)
                      .setType("STRING")
                      .setMode("NULLABLE")));

  private final Relationship relationship;
  private final Hierarchy hierarchy;

  public ComputeRollupCounts(Entity rollupEntity, Relationship relationship, Hierarchy hierarchy) {
    super(rollupEntity);
    this.relationship = relationship;
    this.hierarchy = hierarchy;
  }

  @Override
  public String getName() {
    return "COMPUTE ROLLUPS ("
        + getRollupEntity().getName()
        + ", "
        + relationship.getName()
        + ", "
        + (hierarchy == null ? "NO HIERARCHY" : hierarchy.getName())
        + ")";
  }

  @Override
  public void run(boolean isDryRun, Indexer.Executors executors) {
    // If the temp table hasn't been written yet, run the Dataflow job.
    if (!checkTableExists(getTempTable(), executors.index())) {
      writeFieldsToTempTable(isDryRun, executors);
    } else {
      LOGGER.info("Temp table has already been written.");
    }

    // Dataflow jobs can only write new rows to BigQuery, so in this second step, copy over the
    // rollup information to the corresponding columns in the entity table.
    copyFieldsToEntityTable(isDryRun, executors);
  }

  @Override
  public void clean(boolean isDryRun, Indexer.Executors executors) {
    if (checkTableExists(getTempTable(), executors.index())) {
      deleteTable(getTempTable(), isDryRun, executors.index());
    }
    // CreateEntityTable will delete the entity table, which includes all the rows updated by this
    // job.
  }

  @Override
  public JobStatus checkStatus(Indexer.Executors executors) {
    // Check if the temp table already exists.
    if (!checkTableExists(getTempTable(), executors.index())) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the entity table already exists.
    if (!checkTableExists(getEntityIndexTable(), executors.index())) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the table has at least 1 row where count IS NOT NULL.
    RelationshipMapping indexMapping = relationship.getMapping(Underlay.MappingType.INDEX);
    ColumnSchema countColumnSchema =
        relationship
            .getField(RelationshipField.Type.COUNT, getRollupEntity(), hierarchy)
            .buildColumnSchema();
    return checkOneNotNullRowExists(
            indexMapping.getRollupInfo(getRollupEntity(), hierarchy).getCount(),
            countColumnSchema,
            executors)
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }

  @VisibleForTesting
  public TablePointer getTempTable() {
    // Name the temporary table rollup_[rollup entity]_[counted entity].
    DataPointer dataPointer =
        getRollupEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer().getDataPointer();
    return TablePointer.fromTableName(
        TEMP_TABLE_PREFIX
            + getRollupEntity().getName()
            + "_"
            + getCountedEntity().getName()
            + (hierarchy == null ? "" : "_" + hierarchy.getName()),
        dataPointer);
  }

  private void writeFieldsToTempTable(boolean isDryRun, Indexer.Executors executor) {
    Pipeline pipeline =
        Pipeline.create(buildDataflowPipelineOptions(getBQDataPointer(getTempTable())));

    // Read in the rollup entity ids from BQ.
    Query rollupIds =
        getRollupEntity().getMapping(Underlay.MappingType.SOURCE).queryIds(ID_COLUMN_NAME);
    LOGGER.info("select all rollup entity ids SQL: {}", rollupIds);
    PCollection<Long> rollupIdsPC =
        BigQueryUtils.readNodesFromBQ(
            pipeline, executor.source().renderSQL(rollupIds), "rollupIds");

    // Read in the rollup-counted entity id pairs from BQ.
    boolean rollupEntityIsA = relationship.getEntityA().equals(getEntity());
    String rollupEntityIdAlias = rollupEntityIsA ? ID_COLUMN_NAME : COUNT_ID_COLUMN_NAME;
    String countedEntityIdAlias = rollupEntityIsA ? COUNT_ID_COLUMN_NAME : ID_COLUMN_NAME;
    Query rollupCountedIdPairs =
        relationship
            .getMapping(Underlay.MappingType.SOURCE)
            .queryIdPairs(rollupEntityIdAlias, countedEntityIdAlias);
    LOGGER.info("select all rollup-counted id pairs SQL: {}", rollupCountedIdPairs);
    PCollection<KV<Long, Long>> rollupCountedIdPairsPC =
        BigQueryUtils.readOccurrencesFromBQ(
            pipeline, executor.source().renderSQL(rollupCountedIdPairs));

    // Optionally handle a hierarchy for the rollup entity.
    if (hierarchy != null) {
      SQLExpression rollupAncestorDescendantPairs =
          hierarchy
              .getMapping(Underlay.MappingType.INDEX)
              .queryAncestorDescendantPairs(ANCESTOR_COLUMN_NAME, DESCENDANT_COLUMN_NAME);
      LOGGER.info(
          "select all rollup entity ancestor-descendant id pairs SQL: {}",
          rollupAncestorDescendantPairs);

      // Read in the ancestor-descendant relationships from BQ and build (descendant, ancestor) KV
      // pairs.
      PCollection<KV<Long, Long>> rollupAncestorDescendantKVsPC =
          BigQueryUtils.readAncestorDescendantRelationshipsFromBQ(
              pipeline, executor.source().renderSQL(rollupAncestorDescendantPairs));

      // Expand the set of occurrences to include a repeat for each ancestor.
      rollupCountedIdPairsPC =
          CountUtils.repeatOccurrencesForHierarchy(
              rollupCountedIdPairsPC, rollupAncestorDescendantKVsPC);
    }

    // Count the number of distinct occurrences per primary node.
    PCollection<KV<Long, Long>> nodeCountKVsPC =
        CountUtils.countDistinct(rollupIdsPC, rollupCountedIdPairsPC);

    // Write the (id, count) rows to BQ.
    writeCountsToBQ(nodeCountKVsPC, getTempTable());

    if (!isDryRun) {
      pipeline.run().waitUntilFinish();
    }
  }

  /** Write the {@link KV} pairs (id, rollup_count) to BQ. */
  private static void writeCountsToBQ(
      PCollection<KV<Long, Long>> nodeCountKVs, TablePointer outputBQTable) {
    PCollection<TableRow> nodeCountBQRows =
        nodeCountKVs.apply(
            "build (id, rollup_count, rollup_displayHints) pcollection of BQ rows",
            ParDo.of(
                new DoFn<KV<Long, Long>, TableRow>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, Long> element = context.element();
                    context.output(
                        new TableRow()
                            .set(ID_COLUMN_NAME, element.getKey())
                            .set(ROLLUP_COUNT_COLUMN_NAME, element.getValue())
                            .set(
                                ROLLUP_DISPLAY_HINTS_COLUMN_NAME,
                                "[DISPLAY_HINTS placeholder JSON string]"));
                  }
                }));

    nodeCountBQRows.apply(
        "insert the (id, rollup_count, rollup_displayHints) rows into BQ",
        BigQueryIO.writeTableRows()
            .to(outputBQTable.getPathForIndexing())
            .withSchema(ROLLUP_COUNT_TABLE_SCHEMA)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
            .withMethod(BigQueryIO.Write.Method.FILE_LOADS));
  }

  private void copyFieldsToEntityTable(boolean isDryRun, Indexer.Executors executors) {
    // Build a query for the id-rollup_count-rollup_displayHints tuples that we want to select.
    Query idCountDisplayHintsTuples = queryIdRollupTuples(getTempTable());
    LOGGER.info("select all id-count-displayHints tuples SQL: {}", idCountDisplayHintsTuples);

    // Build a map of (output) update field name -> (input) selected FieldVariable.
    // This map only contains two items, because we're only updating the count and display_hints
    // fields.
    RelationshipMapping indexMapping = relationship.getMapping(Underlay.MappingType.INDEX);
    Map<String, FieldVariable> updateFields = new HashMap<>();

    String updateCountFieldName =
        indexMapping.getRollupInfo(getRollupEntity(), hierarchy).getCount().getColumnName();
    FieldVariable selectCountField =
        idCountDisplayHintsTuples.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(ROLLUP_COUNT_COLUMN_NAME))
            .findFirst()
            .orElseThrow();
    updateFields.put(updateCountFieldName, selectCountField);

    String updateDisplayHintsFieldName =
        indexMapping.getRollupInfo(getRollupEntity(), hierarchy).getDisplayHints().getColumnName();
    FieldVariable selectDisplayHintsField =
        idCountDisplayHintsTuples.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(ROLLUP_DISPLAY_HINTS_COLUMN_NAME))
            .findFirst()
            .orElseThrow();
    updateFields.put(updateDisplayHintsFieldName, selectDisplayHintsField);

    // Check that the count and display_hints fields are not in a different table from the entity
    // table.
    if (indexMapping.getRollupInfo(getRollupEntity(), hierarchy).getCount().isForeignKey()
        || !indexMapping
            .getRollupInfo(getRollupEntity(), hierarchy)
            .getCount()
            .getTablePointer()
            .equals(getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer())) {
      throw new SystemException(
          "Indexing rollup count information only supports an index mapping to a column in the entity table");
    }
    if (indexMapping.getRollupInfo(getRollupEntity(), hierarchy).getDisplayHints().isForeignKey()
        || !indexMapping
            .getRollupInfo(getRollupEntity(), hierarchy)
            .getDisplayHints()
            .getTablePointer()
            .equals(getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer())) {
      throw new SystemException(
          "Indexing rollup display hints information only supports an index mapping to a column in the entity table");
    }

    updateEntityTableFromSelect(
        idCountDisplayHintsTuples, updateFields, ID_COLUMN_NAME, isDryRun, executors);
  }

  public static Query queryIdRollupTuples(TablePointer tablePointer) {
    TableVariable tempTableVar = TableVariable.forPrimary(tablePointer);
    List<TableVariable> inputTables = Lists.newArrayList(tempTableVar);
    FieldVariable selectIdFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(ID_COLUMN_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    FieldVariable selectCountFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(ROLLUP_COUNT_COLUMN_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    FieldVariable selectDisplayHintsFieldVar =
        new FieldPointer.Builder()
            .tablePointer(tablePointer)
            .columnName(ROLLUP_DISPLAY_HINTS_COLUMN_NAME)
            .build()
            .buildVariable(tempTableVar, inputTables);
    return new Query.Builder()
        .select(List.of(selectIdFieldVar, selectCountFieldVar, selectDisplayHintsFieldVar))
        .tables(inputTables)
        .build();
  }

  private Entity getRollupEntity() {
    return getEntity();
  }

  private Entity getCountedEntity() {
    return relationship.getEntityA().equals(getRollupEntity())
        ? relationship.getEntityB()
        : relationship.getEntityA();
  }
}
