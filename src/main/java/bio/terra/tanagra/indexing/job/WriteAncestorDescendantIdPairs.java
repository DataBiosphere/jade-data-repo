package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.ANCESTOR_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.CHILD_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.DESCENDANT_COLUMN_NAME;
import static bio.terra.tanagra.indexing.job.beam.BigQueryUtils.PARENT_COLUMN_NAME;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.indexing.job.beam.GraphUtils;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.HierarchyMapping;
import bio.terra.tanagra.underlay.Underlay;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch Apache Beam pipeline for flattening hierarchical parent-child relationships to
 * ancestor-descendant relationships.
 */
public class WriteAncestorDescendantIdPairs extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteParentChildIdPairs.class);

  // The default table schema for the ancestor-descendant output table.
  private static final TableSchema ANCESTOR_DESCENDANT_TABLE_SCHEMA =
      new TableSchema()
          .setFields(
              List.of(
                  new TableFieldSchema()
                      .setName(ANCESTOR_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED"),
                  new TableFieldSchema()
                      .setName(DESCENDANT_COLUMN_NAME)
                      .setType("INTEGER")
                      .setMode("REQUIRED")));

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
  public void run(boolean isDryRun, QueryExecutor executor) {
    HierarchyMapping sourceHierarchyMapping =
        getEntity().getHierarchy(hierarchyName).getMapping(Underlay.MappingType.SOURCE);
    SQLExpression selectChildParentIdPairs =
        sourceHierarchyMapping.queryChildParentPairs(CHILD_COLUMN_NAME, PARENT_COLUMN_NAME);
    String sql = executor.renderSQL(selectChildParentIdPairs);
    LOGGER.info("select all child-parent id pairs SQL: {}", sql);

    Pipeline pipeline =
        Pipeline.create(buildDataflowPipelineOptions(getBQDataPointer(getAuxiliaryTable())));
    PCollection<KV<Long, Long>> relationships =
        pipeline
            .apply(
                "read parent-child query result",
                BigQueryIO.readTableRows()
                    .fromQuery(sql)
                    .withMethod(BigQueryIO.TypedRead.Method.EXPORT)
                    .usingStandardSql())
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.longs()))
                    .via(WriteAncestorDescendantIdPairs::relationshipRowToKV));
    PCollection<KV<Long, Long>> flattenedRelationships =
        GraphUtils.transitiveClosure(relationships, sourceHierarchyMapping.getMaxHierarchyDepth())
            .apply(Distinct.create()); // There may be duplicate descendants.
    flattenedRelationships
        .apply(ParDo.of(new KVToTableRow()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(getAuxiliaryTable().getPathForIndexing())
                .withSchema(ANCESTOR_DESCENDANT_TABLE_SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_EMPTY)
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    if (!isDryRun) {
      pipeline.run().waitUntilFinish();
    }
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
    return getEntity()
        .getHierarchy(hierarchyName)
        .getMapping(Underlay.MappingType.INDEX)
        .getAncestorDescendant()
        .getTablePointer();
  }

  /** Parses a {@link TableRow} as a row containing a (parent_id, child_id) long pair. */
  private static KV<Long, Long> relationshipRowToKV(TableRow row) {
    // TODO: Support id data types other than longs.
    Long parentId = Long.parseLong((String) row.get(PARENT_COLUMN_NAME));
    Long childId = Long.parseLong((String) row.get(CHILD_COLUMN_NAME));
    return KV.of(parentId, childId);
  }

  /**
   * Converts a KV pair to a BQ TableRow object. This DoFn is defined in a separate static class
   * instead of an anonymous inner (inline) one so that it will be Serializable. Anonymous inner
   * classes include a pointer to the containing class, which here is not Serializable.
   */
  private static class KVToTableRow extends DoFn<KV<Long, Long>, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Long ancestor = context.element().getKey();
      Long descendant = context.element().getValue();
      TableRow row =
          new TableRow()
              .set(ANCESTOR_COLUMN_NAME, ancestor)
              .set(DESCENDANT_COLUMN_NAME, descendant);
      context.output(row);
    }
  }
}
