package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import com.google.cloud.bigquery.TableResult;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.StreamSupport;

public class AggregateBQQueryResultsUtils {
  // TODO - pull real values for hasChildren and count
  public static List<SnapshotBuilderConcept> aggregateConceptResults(TableResult result) {
    return StreamSupport.stream(result.iterateAll().spliterator(), false)
        .map(
            row -> {
              int count;
              try {
                count = (int) row.get("count").getLongValue(); // If exists, use its value
              } catch (IllegalArgumentException e) {
                count = 1;
              }
              return new SnapshotBuilderConcept()
                  .id((int) (row.get("concept_id").getLongValue()))
                  .name(row.get("concept_name").getStringValue())
                  .hasChildren(true)
                  .count(count);
            })
        .toList();
  }

  public static List<Integer> rollupCountsMapper(TableResult result) {
    return StreamSupport.stream(result.iterateAll().spliterator(), false)
        .map(row -> (int) row.get(0).getLongValue())
        .toList();
  }
}
