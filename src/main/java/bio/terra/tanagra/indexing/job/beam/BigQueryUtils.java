package bio.terra.tanagra.indexing.job.beam;

import com.google.api.services.bigquery.model.TableRow;
import java.time.LocalDate;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public final class BigQueryUtils {
  // TODO: Pull these default column names from the underlay HierarchyMapping class, instead of
  // using static names here. Currently these are defined in this utils class because different Beam
  // workflows read the results of previous ones, and make assumptions about what the column names
  // are.
  public static final String ID_COLUMN_NAME = "id";
  public static final String PARENT_COLUMN_NAME = "parent";
  public static final String CHILD_COLUMN_NAME = "child";
  public static final String ANCESTOR_COLUMN_NAME = "ancestor";
  public static final String DESCENDANT_COLUMN_NAME = "descendant";
  public static final String PATH_COLUMN_NAME = "path";
  public static final String NUMCHILDREN_COLUMN_NAME = "num_children";
  public static final String COUNT_ID_COLUMN_NAME = "count_id";
  public static final String ROLLUP_COUNT_COLUMN_NAME = "rollup_count";
  public static final String ROLLUP_DISPLAY_HINTS_COLUMN_NAME = "rollup_displayhints";

  private BigQueryUtils() {}

  /** Read the set of nodes from BQ and build a {@link PCollection} of just the node identifiers. */
  public static PCollection<Long> readNodesFromBQ(
      Pipeline pipeline, String sqlQuery, String description) {
    PCollection<TableRow> allNodesBqRows =
        pipeline.apply(
            "read (id) rows: " + description,
            BigQueryIO.readTableRows()
                .fromQuery(sqlQuery)
                .withMethod(BigQueryIO.TypedRead.Method.EXPORT)
                .usingStandardSql());
    return allNodesBqRows.apply(
        "build (id) pcollection: " + description,
        MapElements.into(TypeDescriptors.longs())
            .via(tableRow -> Long.parseLong((String) tableRow.get(ID_COLUMN_NAME))));
  }

  /**
   * Read all the child-parent relationships from BQ and build a {@link PCollection} of {@link KV}
   * pairs (child, parent).
   */
  public static PCollection<KV<Long, Long>> readChildParentRelationshipsFromBQ(
      Pipeline pipeline, String sqlQuery) {
    return readTwoFieldRowsFromBQ(pipeline, sqlQuery, CHILD_COLUMN_NAME, PARENT_COLUMN_NAME);
  }

  /**
   * Read all the occurrence rows from BQ and build a {@link PCollection} of {@link KV} pairs (id,
   * count_id).
   */
  public static PCollection<KV<Long, Long>> readOccurrencesFromBQ(
      Pipeline pipeline, String sqlQuery) {
    return readTwoFieldRowsFromBQ(pipeline, sqlQuery, ID_COLUMN_NAME, COUNT_ID_COLUMN_NAME);
  }

  /**
   * Read all the ancestor-descendant relationships from BQ and build a {@link PCollection} of
   * {@link KV} pairs (descendant, ancestor).
   */
  public static PCollection<KV<Long, Long>> readAncestorDescendantRelationshipsFromBQ(
      Pipeline pipeline, String sqlQuery) {
    return readTwoFieldRowsFromBQ(pipeline, sqlQuery, DESCENDANT_COLUMN_NAME, ANCESTOR_COLUMN_NAME);
  }

  /**
   * Read all the two-field rows from BQ and build a {@link PCollection} of {@link KV} pairs
   * (field1, field2).
   */
  public static PCollection<KV<Long, Long>> readTwoFieldRowsFromBQ(
      Pipeline pipeline, String sqlQuery, String field1Name, String field2Name) {
    PCollection<TableRow> bqRows =
        pipeline.apply(
            "read all (" + field1Name + ", " + field2Name + ") rows",
            BigQueryIO.readTableRows()
                .fromQuery(sqlQuery)
                .withMethod(BigQueryIO.TypedRead.Method.EXPORT)
                .usingStandardSql());
    return bqRows.apply(
        "build (" + field1Name + ", " + field2Name + ") pcollection",
        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.longs()))
            .via(
                tableRow -> {
                  Long field1 = Long.parseLong((String) tableRow.get(field1Name));
                  Long field2 = Long.parseLong((String) tableRow.get(field2Name));
                  return KV.of(field1, field2);
                }));
  }

  /** Apache Beam returns DATE/TIMESTAMP columns as string. Convert to LocalDate. */
  public static LocalDate toLocalDate(String date) {
    // For DATE column, date looks like "2017-09-13".
    // For TIMESTAMP column, date looks like "1947-02-06 00:00:00 UTC".
    // Convert latter to former, since LocalDate doesn't know how to parse latter.
    return LocalDate.parse(date.substring(0, 10));
  }
}
